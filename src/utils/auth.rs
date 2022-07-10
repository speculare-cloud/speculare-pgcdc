use crate::{AUTHPOOL, CHECKSESSIONS_CACHE};

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

#[async_trait]
impl<B> FromRequest<B> for AuthCookie
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

        let spcks = match cookies.get(COOKIE_NAME) {
            Some(cookie) => cookie,
            None => return Err((StatusCode::UNAUTHORIZED, "no `SP-CKS` found in cookies")),
        };

        let mut value = spcks.value().to_owned().replace("\\\"", "");
        match simd_json::from_str::<Self>(&mut value) {
            Ok(val) => Ok(val),
            Err(_) => Err((
                StatusCode::BAD_REQUEST,
                "cannot find the user_id inside the cookie",
            )),
        }
    }
}

pub async fn restrict_auth(
    auth_cookie: AuthCookie,
    specific: SpecificFilter,
) -> Result<(), ApiError> {
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
