use std::time::Duration;

use crate::{
    utils::specific_filter::{DataType, SpecificFilter},
    CONFIG,
};

use async_trait::async_trait;
use axum::{
    extract::{FromRef, FromRequestParts},
    http::{request::Parts, StatusCode},
};
use axum_extra::extract::SignedCookieJar;
use diesel::{r2d2::ConnectionManager, PgConnection};
use moka::sync::Cache;
use once_cell::sync::Lazy;
use serde::Deserialize;
use sproot::{apierrors::ApiError, as_variant, models::ApiKey, Pool};
use uuid::Uuid;

use super::AppState;

const COOKIE_NAME: &str = "SP-CKS";

static CHECKSESSIONS_CACHE: Lazy<Cache<String, String>> = Lazy::new(|| {
    Cache::builder()
        .time_to_live(Duration::from_secs(60 * 60))
        .build()
});

static CHECKAPI_CACHE: Lazy<Cache<String, Uuid>> = Lazy::new(|| {
    Cache::builder()
        .time_to_live(Duration::from_secs(60 * 60))
        .build()
});

pub static AUTHPOOL: Lazy<Pool> = Lazy::new(|| {
    // Init the connection to the postgresql
    let manager = ConnectionManager::<PgConnection>::new(&CONFIG.auth_database_url);
    // This step might spam for error CONFIG.database_max_connection of times, this is normal.
    match r2d2::Pool::builder()
        .max_size(CONFIG.auth_database_max_connection)
        .min_idle(Some((10 * CONFIG.auth_database_max_connection) / 100))
        .build(manager)
    {
        Ok(pool) => {
            info!("R2D2 PostgreSQL pool created");
            pool
        }
        Err(e) => {
            error!("Failed to create db pool: {}", e);
            std::process::exit(1);
        }
    }
});

#[derive(Debug, Deserialize)]
pub struct AuthCookie {
    pub user_id: String,
}

#[derive(Debug, Deserialize)]
pub struct AuthInfo {
    pub is_admin: bool,
    pub auth_cookie: Option<AuthCookie>,
}

#[async_trait]
impl<B> FromRequestParts<B> for AuthInfo
where
    B: Send + Sync,
    AppState: FromRef<B>,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(req: &mut Parts, state: &B) -> Result<Self, Self::Rejection> {
        // dbg!("Cookies: {:?}", req.headers().get(COOKIE).split(';'));
        let cookies: SignedCookieJar<AppState> =
            match SignedCookieJar::from_request_parts(req, state).await {
                Ok(cookies) => cookies,
                Err(_) => {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        "you need to send a signed cookies along with your request",
                    ))
                }
            };

        let mut is_admin = false;
        let spcks = match cookies.get(COOKIE_NAME) {
            Some(cookie) => Some(cookie),
            None => {
                let adm = req.headers.get("SP-ADM");
                if adm.is_none() || adm.unwrap().to_str().unwrap() != CONFIG.admin_secret {
                    return Err((StatusCode::UNAUTHORIZED, "no `SP-CKS` found in cookies"));
                }

                is_admin = true;
                None
            }
        };

        let auth_cookie = match spcks {
            Some(spcks) => {
                let mut value = spcks.value().to_owned().replace("\\\"", "");
                match unsafe { simd_json::from_str::<AuthCookie>(&mut value) } {
                    Ok(val) => Some(val),
                    Err(_) => {
                        return Err((
                            StatusCode::BAD_REQUEST,
                            "cannot find the user_id inside the cookie",
                        ))
                    }
                }
            }
            None => None,
        };

        Ok(Self {
            is_admin,
            auth_cookie,
        })
    }
}

pub async fn restrict_auth(auth: AuthInfo, specific: SpecificFilter) -> Result<(), ApiError> {
    let auth_cookie = auth.auth_cookie.unwrap();

    let sp_value = match as_variant!(specific.value, DataType::String) {
        Some(val) => val,
        None => {
            return Err(ApiError::InvalidRequestError(None));
        }
    };

    if specific.column == "host_uuid" || specific.column == "uuid" {
        // Parse the user_id into a UUID
        let uuid = match Uuid::parse_str(&auth_cookie.user_id) {
            Ok(uuid) => uuid,
            Err(_) => {
                return Err(ApiError::InvalidRequestError(None));
            }
        };

        // Check if value is present in the cache, otherwise check the database
        let cuid = auth_cookie.user_id.clone();
        if CHECKSESSIONS_CACHE.get(&sp_value) == Some(auth_cookie.user_id) {
            trace!("CheckSessions: cache hit for {}", &sp_value);
            return Ok(());
        }

        // Get a conn from the auth_db's pool
        let mut conn = match AUTHPOOL.get() {
            Ok(conn) => conn,
            Err(err) => {
                error!("failed to get a conn: {}", err);
                return Err(ApiError::ServerError(None));
            }
        };

        let csp = sp_value.clone();
        let exists = tokio::task::spawn_blocking(move || {
            ApiKey::exists_by_owner_and_host(&mut conn, &uuid, &sp_value)
        })
        .await
        .map_err(|_| ApiError::ServerError(None))??;

        if exists {
            CHECKSESSIONS_CACHE.insert(csp, cuid);
            return Ok(());
        }

        return Err(ApiError::AuthorizationError(None));
    }

    if specific.column == "customer_id" && sp_value == auth_cookie.user_id {
        return Ok(());
    }

    if specific.column == "key" {
        // Parse the user_id into a UUID
        let uuid = match Uuid::parse_str(&auth_cookie.user_id) {
            Ok(uuid) => uuid,
            Err(_) => {
                return Err(ApiError::InvalidRequestError(None));
            }
        };

        // If the keys exists in the cache but it's not for the same user, error
        if let Some(cached) = CHECKAPI_CACHE.get(&sp_value) {
            if cached == uuid {
                trace!("CheckSessions: cache hit for {}", &sp_value);
                return Ok(());
            } else {
                return Err(ApiError::AuthorizationError(None));
            }
        }

        // Get a conn from the auth_db's pool
        let mut conn = match AUTHPOOL.get() {
            Ok(conn) => conn,
            Err(err) => {
                error!("failed to get a conn: {}", err);
                return Err(ApiError::ServerError(None));
            }
        };

        let csp = sp_value.clone();
        let exists = tokio::task::spawn_blocking(move || {
            ApiKey::exists_by_owner_and_key(&mut conn, &uuid, &sp_value)
        })
        .await
        .map_err(|_| ApiError::ServerError(None))??;

        if exists {
            CHECKAPI_CACHE.insert(csp, uuid);
            return Ok(());
        }

        return Err(ApiError::AuthorizationError(None));
    }

    Err(ApiError::AuthorizationError(None))
}
