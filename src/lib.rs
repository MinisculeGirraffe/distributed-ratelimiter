use aws_sdk_dynamodb::{
    error::SdkError,
    operation::{put_item::PutItemError, query::QueryError},
    types::{AttributeValue, Select},
    Client,
};
use aws_smithy_runtime_api::http::Response;
use aws_smithy_types::body::SdkBody;
use serde::{Deserialize, Serialize};
use serde_dynamo::{aws_sdk_dynamodb_1::to_item, from_item};
use std::{
    cmp,
    num::NonZeroU64,
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
/// The settings for a rate limit
pub struct RateLimitSettings {
    /// The maximum number of tokens that can be stored
    pub max_tokens: u64,
    /// The number of tokens to start with
    pub starting_tokens: u64,
    /// The number of tokens to add every `refill_interval`
    pub refill_rate: u64,
    /// The number of seconds between refills
    pub refill_interval: NonZeroU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
/// A single item in the cache
pub struct RateLimitItem {
    /// The last time the tokens were updated in unix time
    pub last_updated: u64,
    /// The number of tokens remaining
    pub tokens: u64,
}

impl RateLimitItem {
    fn new(tokens: u64) -> Self {
        Self {
            last_updated: current_unix_time(),
            tokens,
        }
    }
}

/// Primary abstraction to decouple the cache from the rate limiter
/// This allows for the cache to be in redis, dynamodb, etc
/// Currently only dynamodb is supported
pub trait TokenBucketClient {
    type Error;
    /// Get the current limit and settings from the cache
    /// If the limit or settings are not in the cache, the default settings will be used
    /// If the limit is not in the cache, a new limit will be created with the starting tokens
    fn get(
        &self,
        id: &str,
        default_settings: RateLimitSettings,
    ) -> impl std::future::Future<Output = Result<(RateLimitItem, RateLimitSettings), Self::Error>> + Send;

    /// Put a new limit into the cache
    fn put_limit(
        &self,
        id: &str,
        limit: RateLimitItem,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send;
    /// Put a new settings into the cache
    fn put_settings(
        &self,
        id: &str,
        settings: RateLimitSettings,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send;
}

#[derive(Debug, Clone)]
/// DynamoDB client for the token bucket
/// The table must have a primary key with the name `pk_name` and a sort key with the name `sk_name`
pub struct TokenDynamoClient {
    /// The name of the DynamoDB table
    pub table_name: String,
    /// The name of the primary key
    pub pk_name: String,
    /// The prefix to add to the primary key
    pub pk_prefix: Option<String>,
    /// The name of the sort key
    pub sk_name: String,
    pub client: Client,
}

impl TokenDynamoClient {
    fn format_pk(&self, id: &str) -> AttributeValue {
        match &self.pk_prefix {
            Some(prefix) => AttributeValue::S(format!("{prefix}{id}")),
            None => AttributeValue::S(id.into()),
        }
    }
}

impl TokenBucketClient for TokenDynamoClient {
    type Error = TokenBucketError;
    async fn get(
        &self,
        id: &str,
        default_settings: RateLimitSettings,
    ) -> Result<(RateLimitItem, RateLimitSettings), Self::Error> {
        let items = self
            .client
            .query()
            .key_condition_expression("#key = :value")
            .expression_attribute_names("#key", &self.pk_name)
            .expression_attribute_values(":value", self.format_pk(id))
            .select(Select::AllAttributes)
            .send()
            .await?
            .items
            .unwrap_or_default();

        let mut limit: Option<RateLimitItem> = None;
        let mut settings: Option<RateLimitSettings> = None;
        for item in items {
            match (item.get(&self.sk_name), &limit, &settings) {
                (Some(AttributeValue::S(value)), None, _) if { value == "LIMIT" } => {
                    limit = from_item(item).ok()
                }
                (Some(AttributeValue::S(value)), _, None) if { value == "SETTINGS" } => {
                    settings = from_item(item).ok()
                }
                (Some(_), Some(_), Some(_)) => break,
                _ => continue,
            }
        }
        let settings = settings.unwrap_or(default_settings);
        let limit = limit.unwrap_or_else(|| RateLimitItem::new(settings.starting_tokens));

        Ok((limit, settings))
    }

    async fn put_limit(&self, id: &str, limit: RateLimitItem) -> Result<(), Self::Error> {
        let last_updated = limit.last_updated.to_string();
        let item = to_item(limit)?;

        let result = self
            .client
            .put_item()
            .table_name(&self.table_name)
            .set_item(Some(item))
            .item(&self.pk_name, self.format_pk(id))
            .item(&self.sk_name, AttributeValue::S("LIMIT".into()))
            .condition_expression("last_updated <= :new_updated")
            .expression_attribute_values(":new_updated", AttributeValue::N(last_updated))
            .send()
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(SdkError::ServiceError(s)) => match s.err() {
                // This can fail if the limit was updated by another request
                // This is fine, we just want to make sure we don't overwrite a newer limit
                // Something something eventually consistent
                PutItemError::ConditionalCheckFailedException(_) => Ok(()),
                _ => Err(TokenBucketError::DynamoPut(SdkError::ServiceError(s))),
            },
            Err(e) => Err(TokenBucketError::DynamoPut(e)),
        }
    }

    async fn put_settings(&self, id: &str, settings: RateLimitSettings) -> Result<(), Self::Error> {
        let item = to_item(settings)?;
        self.client
            .put_item()
            .table_name(&self.table_name)
            .set_item(Some(item))
            .item(&self.pk_name, self.format_pk(id))
            .item(&self.sk_name, AttributeValue::S("SETTINGS".into()))
            .send()
            .await?;

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum LimitResult {
    Allow { remaining: u64 },
    Deny,
}

pub struct TokenBucket<T: TokenBucketClient> {
    client: T,
    pub default_settings: RateLimitSettings,
}

impl<T: TokenBucketClient> TokenBucket<T> {
    pub fn new(client: T, default_settings: RateLimitSettings) -> Result<Self, TokenBucketError> {
        Ok(Self {
            client,
            default_settings,
        })
    }

    pub async fn limit(&self, id: &str, cost: u64) -> Result<LimitResult, T::Error> {
        let (mut limit, settings) = self.client.get(id, self.default_settings).await?;

        let now = current_unix_time();
        //todo check divide by zero
        let intervals = now
            .saturating_sub(limit.last_updated)
            .saturating_div(settings.refill_interval.into());

        let refilled_tokens = intervals * settings.refill_rate;
        limit.tokens = cmp::min(settings.max_tokens, limit.tokens + refilled_tokens);

        if limit.tokens < cost {
            return Ok(LimitResult::Deny);
        }

        limit.tokens = limit.tokens.saturating_sub(cost);
        let remaining = limit.tokens;

        self.client.put_limit(id, limit).await?;
        Ok(LimitResult::Allow { remaining })
    }
}

pub(crate) fn current_unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time Went Backwards")
        .as_secs()
}

#[derive(Error, Debug)]
pub enum TokenBucketError {
    #[error("Failed to get")]
    DynamoGet(#[from] SdkError<QueryError, Response<SdkBody>>),
    #[error("Failed to Update")]
    DynamoPut(#[from] SdkError<PutItemError, Response<SdkBody>>),
    #[error("Failed to serialize/deserialize the dynamodb item")]
    SerdeError(#[from] serde_dynamo::Error),
}
