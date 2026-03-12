use async_trait::async_trait;

use crate::{CONFIG, error::VfResult, notifier::wecom::WecomNotifier};

pub mod wecom;

#[derive(Debug, Clone)]
pub enum NotificationType {
    BacktestErrors,
    BacktestTodayUpdates,
}

#[derive(Debug, Clone)]
pub struct Notification {
    pub r#type: NotificationType,
    pub title: String,
    pub content: String,
}

#[async_trait]
trait Notifier: Send {
    async fn send(&self, notification: &Notification) -> VfResult<()>;
}

pub async fn notify(notification: &Notification) -> VfResult<()> {
    let config = { CONFIG.read().await };

    if !config.wecom_webhook.is_empty() {
        let notifier = WecomNotifier::new(&config.wecom_webhook);
        notifier.send(notification).await?;
    }

    Ok(())
}
