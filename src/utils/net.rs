use std::{collections::HashMap, time::Duration};

use reqwest::Method;
use reqwest_middleware::ClientBuilder;
use reqwest_retry::{Jitter, RetryTransientMiddleware, policies::ExponentialBackoff};
use url::Url;

use crate::error::{VfError, VfResult};

pub async fn http_get(
    url: &str,
    path: Option<&str>,
    query: Option<HashMap<String, String>>,
    headers: Option<HashMap<String, String>>,
    timeout_secs: u64,
    max_retries: u32,
) -> VfResult<Vec<u8>> {
    let request_url = if let Some(path) = path {
        &join_url(url, path)?
    } else {
        url
    };

    let client = create_retry_client(timeout_secs, max_retries);

    let mut request_builder = client
        .request(Method::GET, request_url)
        .timeout(Duration::from_secs(timeout_secs));

    if let Some(query) = query.as_ref() {
        request_builder = request_builder.query(query);
    }

    if let Some(headers) = headers {
        for (k, v) in headers {
            request_builder = request_builder.header(k, v);
        }
    }

    let response = request_builder.send().await?;

    if response.status().is_success() {
        Ok(response.bytes().await?.to_vec())
    } else {
        Err(VfError::HttpStatusError {
            status: response.status().to_string(),
            request: format!(
                "{}?{}",
                request_url,
                query
                    .unwrap_or_default()
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join("&")
            ),
        })
    }
}

pub async fn http_post(
    url: &str,
    path: Option<&str>,
    body: &serde_json::Value,
    headers: Option<HashMap<String, String>>,
    timeout_secs: u64,
    max_retries: u32,
) -> VfResult<Vec<u8>> {
    let request_url = if let Some(path) = path {
        &join_url(url, path)?
    } else {
        url
    };

    let client = create_retry_client(timeout_secs, max_retries);

    let mut request_builder = client
        .request(Method::POST, request_url)
        .timeout(Duration::from_secs(timeout_secs))
        .body(body.to_string());

    if let Some(headers) = headers {
        for (k, v) in headers {
            request_builder = request_builder.header(k, v);
        }
    }

    let response = request_builder.send().await?;

    if response.status().is_success() {
        Ok(response.bytes().await?.to_vec())
    } else {
        Err(VfError::HttpStatusError {
            status: response.status().to_string(),
            request: format!("{request_url}?{body}"),
        })
    }
}

pub fn join_url(base_url: &str, extend_url: &str) -> Result<String, url::ParseError> {
    let mut url = Url::parse(base_url)?;

    url.path_segments_mut()
        .map_err(|_| url::ParseError::RelativeUrlWithCannotBeABaseBase)?
        .pop_if_empty()
        .extend(extend_url.split('/').filter(|s| !s.is_empty()));

    Ok(url.to_string())
}

fn create_retry_client(
    timeout_secs: u64,
    max_retries: u32,
) -> reqwest_middleware::ClientWithMiddleware {
    let retry_policy = ExponentialBackoff::builder()
        .retry_bounds(Duration::from_secs(1), Duration::from_secs(timeout_secs))
        .jitter(Jitter::Bounded)
        .base(2)
        .build_with_total_retry_duration_and_max_retries(
            Duration::from_secs(max_retries as u64 * timeout_secs),
            max_retries,
        );
    ClientBuilder::new(reqwest::Client::new())
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_join_url() {
        assert_eq!(
            join_url("http://127.0.0.1:8000/", "/hello").unwrap(),
            "http://127.0.0.1:8000/hello"
        );
    }
}
