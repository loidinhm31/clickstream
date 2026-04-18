# API Reference

Complete REST API documentation for the Clickstream Ingestion API.

## Ingestion Endpoints

### POST /api/events

Ingest a single click event. Event is validated and published asynchronously to Kafka.

**Request:**
```http
POST /api/events HTTP/1.1
Host: localhost:8081
Content-Type: application/json

{
  "eventId": "550e8400-e29b-41d4-a716-446655440000",
  "userId": "user-abc-123",
  "sessionId": "sess-xyz-789",
  "eventType": "CLICK",
  "targetElement": "button#submit-order",
  "pageUrl": "https://app.example.com/checkout",
  "referrerUrl": "https://app.example.com/cart",
  "timestamp": 1712678400000,
  "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
  "metadata": {
    "x": 450,
    "y": 320,
    "elementText": "Submit"
  }
}
```

**Response: 202 Accepted**
```json
{
  "success": true,
  "message": "Event accepted",
  "errors": null
}
```

**Response: 400 Bad Request** (validation failed)
```json
{
  "success": false,
  "message": "Validation failed",
  "errors": [
    "userId must not be blank",
    "timestamp must not be null"
  ]
}
```

**Status Codes:**
| Code | Meaning |
|------|---------|
| 202 | Event accepted and queued for Kafka publish |
| 400 | Validation error (check `errors` array) |
| 429 | Rate limit exceeded |
| 500 | Server error (Kafka publish failed) |

**Validation Rules:**
- `eventId` - Must be a valid UUID
- `userId`, `sessionId` - Must not be blank
- `eventType` - Must be valid enum (CLICK, PAGE_VIEW, FORM_SUBMIT, etc.)
- `pageUrl` - Must be valid HTTP(S) URL
- `timestamp` - Must not be null, should be recent (< 24h old)

### POST /api/events/batch

Ingest multiple events in a single request (up to 100 events).

**Request:**
```http
POST /api/events/batch HTTP/1.1
Host: localhost:8081
Content-Type: application/json

[
  {
    "eventId": "e1",
    "userId": "user1",
    "sessionId": "sess1",
    "eventType": "PAGE_VIEW",
    "pageUrl": "https://app.example.com/home",
    "timestamp": 1712678400000
  },
  {
    "eventId": "e2",
    "userId": "user1",
    "sessionId": "sess1",
    "eventType": "CLICK",
    "targetElement": "a.link",
    "pageUrl": "https://app.example.com/home",
    "timestamp": 1712678401000
  }
]
```

**Response: 202 Accepted**
```json
{
  "success": true,
  "message": "Batch of 2 events accepted",
  "errors": null
}
```

**Response: 400 Bad Request** (batch too large or validation failed)
```json
{
  "success": false,
  "message": "Batch validation failed",
  "errors": [
    "Batch size 150 exceeds maximum 100",
    "Event at index 5: userId must not be blank"
  ]
}
```

**Constraints:**
- Maximum 100 events per batch
- All events must pass individual validation
- If any event fails validation, entire batch is rejected
- Typical batch: < 50ms latency to 202 response

---

## Analytics Endpoints

### GET /api/analytics/sessions

Query session aggregates with optional filters and pagination.

**Request:**
```http
GET /api/analytics/sessions?page=0&size=20&userId=user1&startTime=2026-04-18T00:00:00Z&endTime=2026-04-19T00:00:00Z
Host: localhost:8081
```

**Query Parameters:**

| Parameter | Type | Required | Default | Example |
|-----------|------|----------|---------|---------|
| `page` | integer | No | 0 | `0` |
| `size` | integer | No | 20 | `50` (max: 100) |
| `userId` | string | No | - | `user-abc-123` |
| `startTime` | ISO-8601 timestamp | No | - | `2026-04-18T00:00:00Z` |
| `endTime` | ISO-8601 timestamp | No | - | `2026-04-19T00:00:00Z` |

**Response: 200 OK**
```json
{
  "content": [
    {
      "id": "ObjectId",
      "userId": "user1",
      "sessionId": "sess1",
      "sessionDurationMs": 45000,
      "eventCount": 12,
      "pageViews": 3,
      "clickCount": 8,
      "formSubmits": 1,
      "startTime": 1712678400000,
      "endTime": 1712678445000,
      "devices": ["mobile"],
      "browsers": ["Chrome"],
      "firstPageUrl": "https://app.example.com/home",
      "lastPageUrl": "https://app.example.com/checkout",
      "bounceRate": 0.0
    }
  ],
  "pageable": {
    "pageNumber": 0,
    "pageSize": 20,
    "totalElements": 150,
    "totalPages": 8,
    "hasPrevious": false,
    "hasNext": true
  }
}
```

**Filter Combinations:**
- No filters: Returns all sessions (paginated)
- `userId` only: All sessions for user
- `startTime` + `endTime`: Sessions in date range
- All three: Sessions for specific user in date range

### GET /api/analytics/pages

Query page metrics and performance data.

**Request:**
```http
GET /api/analytics/pages?page=0&size=20&pageUrl=https://app.example.com/checkout
Host: localhost:8081
```

**Query Parameters:**

| Parameter | Type | Required | Default |
|-----------|------|----------|---------|
| `page` | integer | No | 0 |
| `size` | integer | No | 20 |
| `pageUrl` | string | No | - |
| `startTime` | ISO-8601 | No | - |
| `endTime` | ISO-8601 | No | - |

**Response: 200 OK**
```json
{
  "content": [
    {
      "id": "ObjectId",
      "pageUrl": "https://app.example.com/checkout",
      "pageTitle": "Checkout - Example App",
      "viewCount": 1250,
      "uniqueVisitors": 480,
      "avgSessionDurationMs": 45000,
      "bounceRate": 0.15,
      "exitRate": 0.22,
      "avgScrollDepth": 0.68,
      "conversionRate": 0.08,
      "topReferrers": [
        { "url": "https://app.example.com/products", "count": 580 },
        { "url": "https://app.example.com/home", "count": 340 }
      ],
      "topClickTargets": [
        { "element": "button#submit-order", "count": 820 },
        { "element": "a.continue-shopping", "count": 180 }
      ],
      "lastUpdated": 1712678400000
    }
  ],
  "pageable": {
    "pageNumber": 0,
    "pageSize": 20,
    "totalElements": 450,
    "totalPages": 23,
    "hasNext": true
  }
}
```

### GET /api/analytics/journeys/{userId}

Query the user journey map - sequence of pages visited in a session.

**Request:**
```http
GET /api/analytics/journeys/user-abc-123?sessionId=sess-xyz-789
Host: localhost:8081
```

**Path Parameters:**

| Parameter | Type | Required | Example |
|-----------|------|----------|---------|
| `userId` | string | Yes | `user-abc-123` |

**Query Parameters:**

| Parameter | Type | Required |
|-----------|------|----------|
| `sessionId` | string | No |
| `limit` | integer | No (default: 10 sessions) |

**Response: 200 OK**
```json
{
  "userId": "user-abc-123",
  "sessions": [
    {
      "sessionId": "sess-xyz-789",
      "startedAt": 1712678400000,
      "pages": [
        {
          "sequence": 1,
          "pageUrl": "https://app.example.com/home",
          "title": "Home",
          "durationMs": 8000,
          "clicks": 3
        },
        {
          "sequence": 2,
          "pageUrl": "https://app.example.com/products",
          "title": "Products",
          "durationMs": 15000,
          "clicks": 8
        },
        {
          "sequence": 3,
          "pageUrl": "https://app.example.com/checkout",
          "title": "Checkout",
          "durationMs": 22000,
          "clicks": 5
        }
      ]
    }
  ]
}
```

---

## Error Responses

### 400 Bad Request

**Validation Error:**
```json
{
  "timestamp": "2026-04-18T10:30:00Z",
  "status": 400,
  "error": "Bad Request",
  "message": "Validation failed",
  "details": {
    "userId": "must not be blank",
    "eventType": "must be of enum type EventType"
  }
}
```

### 429 Too Many Requests

**Rate Limit Exceeded:**
```json
{
  "timestamp": "2026-04-18T10:30:00Z",
  "status": 429,
  "error": "Too Many Requests",
  "message": "Rate limit exceeded for endpoint /api/events",
  "retryAfter": 5
}
```

### 500 Internal Server Error

**Server Error:**
```json
{
  "timestamp": "2026-04-18T10:30:00Z",
  "status": 500,
  "error": "Internal Server Error",
  "message": "Failed to publish event to Kafka"
}
```

---

## Response Headers

All responses include:

| Header | Value | Example |
|--------|-------|---------|
| `Content-Type` | `application/json` | `application/json; charset=UTF-8` |
| `X-Request-ID` | UUID | `f2e4af99-b7d3-404f-ad00-5f0d6fce8e97` |
| `Date` | RFC 7231 format | `Mon, 18 Apr 2026 10:30:00 GMT` |

Endpoints supporting CORS also include:
| Header | Value |
|--------|-------|
| `Access-Control-Allow-Origin` | `http://localhost:3000` |
| `Access-Control-Allow-Methods` | `GET, POST, OPTIONS` |
| `Access-Control-Allow-Headers` | `Content-Type, Authorization` |

---

## Pagination

List endpoints (`/sessions`, `/pages`, `/journeys`) use Spring Data pagination:

**Response Structure:**
```json
{
  "content": [...],
  "pageable": {
    "pageNumber": 0,
    "pageSize": 20,
    "totalElements": 500,
    "totalPages": 25,
    "hasPrevious": false,
    "hasNext": true
  }
}
```

**Constraints:**
- `page`: 0-indexed (page=0 is first page)
- `size`: 1-100 (default 20)
- Requesting page beyond available pages returns empty `content` with correct `pageable` metadata
