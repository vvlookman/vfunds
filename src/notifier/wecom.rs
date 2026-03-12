use async_trait::async_trait;
use serde_json::json;

use crate::{error::VfResult, notifier::*, utils::net::http_post};

pub struct WecomNotifier {
    webhook_url: String,
}

impl WecomNotifier {
    pub fn new(webhook_url: &str) -> Self {
        Self {
            webhook_url: webhook_url.to_string(),
        }
    }
}

#[async_trait]
impl Notifier for WecomNotifier {
    async fn send(&self, notification: &Notification) -> VfResult<()> {
        let body = json!({
            "msgtype": "text",
            "text": {
                "content": format!("{}\n\n{}", notification.title, notification.content),
            },
        });

        let _ = http_post(&self.webhook_url, None, &body, None, 60, 3).await?;

        Ok(())
    }
}
