use crate::{AUTHPOOL, CHECKSESSIONS_CACHE, CONFIG};

use async_trait::async_trait;
use axum::{
    extract::{FromRequest, RequestParts},
    http::StatusCode,
};
use axum_extra::extract::SignedCookieJar;
use serde::Deserialize;
use sproot::{apierrors::ApiError, as_variant, models::ApiKey};
use uuid::Uuid;

use super::specific_filter::{DataType, SpecificFilter};

const COOKIE_NAME: &str = "SP-CKS";

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
impl<B> FromRequest<B> for AuthInfo
where
    B: Send,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request(req: &mut RequestParts<B>) -> Result<Self, Self::Rejection> {
        // dbg!("Cookies: {:?}", req.headers().get(COOKIE).split(';'));
        let cookies: SignedCookieJar = match SignedCookieJar::from_request(req).await {
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
                let adm = req.headers().get("SP-ADM");
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
                match simd_json::from_str::<AuthCookie>(&mut value) {
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
        let exists =
            tokio::task::block_in_place(move || ApiKey::entry_exists(&mut conn, &uuid, &sp_value))?;

        if exists {
            CHECKSESSIONS_CACHE.insert(csp, cuid).await;
            return Ok(());
        }

        return Err(ApiError::AuthorizationError(None));
    }

    if specific.column == "customer_id" && sp_value == auth_cookie.user_id {
        return Ok(());
    }

    Err(ApiError::AuthorizationError(None))
}
