# Distributed Ratelimit

A distributed rate-limiter intended for use inside of AWS Lambda backed by DynamoDB.
Backed by an eventually TokenBucket.

Supports having per-user/id/operation rate limit settings.

## Expected Latency

Both querying and updating DynamoDB have fairly consistent single digit millisecond latency. Therefore any calls to `.limit()` should add at worst ~20ms of expected latency to whatever operation it is being used to guard.

## Cost Estimate

Some napkin math using [On Demand Pricing](https://aws.amazon.com/dynamodb/pricing/on-demand/?refid=ft_card) for us-east-1.

- Write Request Units (WRU) $1.25 per million write request units
- Read Request Units (RRU) $0.25 per million read request units

`RateLimitItem`: 71 bytes.

```json
{
  "pk": {
    "S": "c87ee0b7-d8bb-4a25-b8d2-507aa9b4d63c"
  },
  "sk": {
    "S": "LIMIT"
  },
  "last_updated": {
    "N": "1705788598"
  },
  "tokens": {
    "N": "10000"
  }
}
```

`RateLimitSettings`:  90 Bytes

```json
{
  "pk": {
    "S": "c87ee0b7-d8bb-4a25-b8d2-507aa9b4d63c"
  },
  "sk": {
    "S": "SETTINGS"
  },
  "max_tokens": {
    "N": "1000"
  },
  "refill_rate": {
    "N": "100"
  },
  "refill_interval": {
    "N": "60"
  }
}
```

- Total size: **161 bytes**

Both the stored rate limit and the settings can be retrieved in a single RCU so long as the supplied PK isn't insanely long.

Read: 1 RCU

Write: 1 RCU

Total Cost: ~$1.50 per million limit requests. Probably a little less.
