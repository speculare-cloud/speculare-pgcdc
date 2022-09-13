#[cfg(feature = "auth")]
use axum::extract::FromRef;
#[cfg(feature = "auth")]
use axum_extra::extract::cookie::Key;

#[cfg(feature = "auth")]
pub mod auth;
pub mod query;
pub mod server;
pub mod ws_handler;
pub mod ws_utils;

#[cfg(feature = "auth")]
#[derive(Clone)]
struct AppState {
    key: Key,
}

#[cfg(feature = "auth")]
impl FromRef<AppState> for Key {
    fn from_ref(state: &AppState) -> Self {
        state.key.clone()
    }
}

#[cfg(feature = "auth")]
impl From<AppState> for Key {
    fn from(value: AppState) -> Self {
        value.key
    }
}
