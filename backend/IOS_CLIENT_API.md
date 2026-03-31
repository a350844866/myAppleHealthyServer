# iOS Client API Notes

这份文档给 `myAppleHealthyBridge` 或后续 iOS App 使用，目标是让客户端先对当前服务端的真实契约有完整概念。

## 当前服务端范围

当前已经落地的客户端相关能力：

- `POST /ingest`
- `GET /api/device-sync-state`
- `GET /api/device-sync-state/anchors`
- `GET /api/records/recent`

当前还没有落地的能力：

- 服务端下发式配置
- 告警规则接口
- 删除样本 / 回滚同步接口
- workout 专用 ingest

目前 ingest 只接受普通 sample，客户端不要先发 workout/event/route。

## Base URL

服务端根地址由部署环境决定，客户端配置的是根地址，不要把路径写进 base URL。

示例：

- 正确：`http://your-server-host:18000`
- 请求地址：`POST http://your-server-host:18000/ingest`

不要写成：

- `http://your-server-host:18000/ingest`

## 认证

服务端支持可选 Bearer Token。

- 如果服务端没有设置 `INGEST_API_TOKEN`，客户端可以不带 `Authorization`
- 如果服务端设置了 `INGEST_API_TOKEN`，客户端必须带：

```http
Authorization: Bearer <token>
```

认证失败时会返回：

```json
{
  "detail": "无效的 ingest token"
}
```

HTTP 状态码：`401`

## POST /ingest

### 请求头

```http
Content-Type: application/json
Authorization: Bearer <token>   // 可选，取决于服务端配置
```

### 请求体

```json
{
  "device_id": "iphone-15-pro-max",
  "bundle_id": "com.example.myAppleHealthyBridge",
  "sent_at": "2026-03-31T08:30:00Z",
  "items": [
    {
      "source": "healthkit",
      "kind": "sample",
      "type": "HKQuantityTypeIdentifierHeartRate",
      "uuid": "11111111-2222-3333-4444-555555555555",
      "start_at": "2026-03-31T08:28:00Z",
      "end_at": "2026-03-31T08:28:00Z",
      "value": 72,
      "unit": "count/min",
      "metadata": {
        "source_name": "Apple Watch",
        "source_version": "11.4",
        "source_bundle_id": "com.apple.health.1234567890",
        "product_type": "Watch7,5"
      }
    }
  ],
  "anchors": {
    "HKQuantityTypeIdentifierHeartRate": "base64-anchor"
  }
}
```

### 字段说明

- `device_id`: 客户端稳定设备标识。服务端用它归并设备同步状态
- `bundle_id`: iOS App 的 bundle id
- `sent_at`: 客户端发送时间，ISO 8601 datetime
- `items`: 本次上送的样本数组
- `anchors`: 每个 HealthKit type 对应一个 anchor 字符串

`items[]` 字段：

- `source`: 当前建议固定为 `healthkit`
- `kind`: 当前只支持 `sample`
- `type`: HealthKit 类型标识，例如 `HKQuantityTypeIdentifierHeartRate`
- `uuid`: 样本唯一标识，必须稳定
- `start_at`: 样本开始时间
- `end_at`: 样本结束时间
- `value`: 数值型样本的值；分类样本可留空
- `unit`: 单位，例如 `count/min`、`%`、`count`
- `metadata`: 透传补充字段，建议尽量带上源设备与源应用信息

### 当前 metadata 建议字段

这些不是强制，但建议 iOS 侧统一提供：

- `source_name`
- `source_version`
- `source_bundle_id`
- `product_type`
- `category_value_label`
- `category_value_raw`

说明：

- 如果是 `SleepAnalysis` 之类的分类型数据，当前服务端会优先把 `category_value_label` 或 `category_value_raw` 写到 `value_text`
- `value` 可以为空，但 `uuid`、时间区间、`type` 要稳定

### 成功响应

```json
{
  "ok": true,
  "accepted": 120,
  "deduplicated": 30
}
```

含义：

- `accepted`: 本次收到的样本数
- `deduplicated`: 因幂等去重而未新增写入的样本数

新增写入数可按 `accepted - deduplicated` 理解。

### 失败响应

`400 Bad Request`

出现于客户端发送了当前不支持的 `kind`：

```json
{
  "detail": "暂不支持的 ingest kind: workout"
}
```

`401 Unauthorized`

```json
{
  "detail": "无效的 ingest token"
}
```

`500 Internal Server Error`

```json
{
  "detail": "ingest failed: <error>"
}
```

## 幂等规则

服务端当前使用这组字段生成幂等键：

- `device_id`
- `bundle_id`
- `item.type`
- `item.uuid`

客户端要求：

- 同一个 HealthKit sample 重试上送时，`uuid` 不能变
- `device_id` 应该稳定
- `bundle_id` 不要随构建环境频繁变化

只要这些关键字段不变，重复提交不会重复写入。

## 服务端当前如何存储

`POST /ingest` 会做三件事：

1. 原始批次写入 `ingest_events`
2. 样本去重后写入 `health_records`
3. 同步状态与 anchors 写入 `device_sync_state`、`device_sync_anchors`

这意味着客户端可以把 `/api/device-sync-state` 当成联调排查入口，把 `/api/device-sync-state/anchors` 当成服务端游标恢复入口。

## GET /api/records/recent

用于按 `device_id` 查看最近落库的原始 `health_records` 明细，方便排查“客户端到底传上来了哪些样本”。

### 查询参数

- `device_id` 必填
- `bundle_id` 选填
- `type` 选填
- `start` 选填，按 `local_date` 过滤，格式 `YYYY-MM-DD`
- `end` 选填，按 `local_date` 过滤，格式 `YYYY-MM-DD`
- `limit` 选填，默认 `100`，最大 `1000`
- `offset` 选填，默认 `0`

### 请求示例

```text
GET /api/records/recent?device_id=jiaxu-iphone&limit=20
```

或：

```text
GET /api/records/recent?device_id=jiaxu-iphone&type=HKQuantityTypeIdentifierHeartRate&start=2026-03-24&end=2026-03-31&limit=50
```

### 响应示例

```json
{
  "total": 2,
  "data": [
    {
      "id": 123456,
      "type": "HKQuantityTypeIdentifierHeartRate",
      "source_name": "Apple Watch",
      "source_version": "11.4",
      "unit": "count/min",
      "value_text": "72",
      "value_num": 72,
      "start_at": "2026-03-31T08:28:00",
      "end_at": "2026-03-31T08:28:00",
      "local_date": "2026-03-31",
      "metadata": {
        "bridge_bundle_id": "com.example.myAppleHealthyBridge",
        "bridge_device_id": "jiaxu-iphone",
        "bridge_kind": "sample",
        "bridge_sent_at": "2026-03-31T08:30:00+08:00",
        "bridge_source": "healthkit",
        "source_name": "Apple Watch"
      },
      "bridge_device_id": "jiaxu-iphone",
      "bridge_bundle_id": "com.example.myAppleHealthyBridge",
      "bridge_sent_at": "2026-03-31T08:30:00+08:00",
      "bridge_kind": "sample",
      "bridge_source": "healthkit"
    }
  ]
}
```

## GET /api/device-sync-state

用于查看最近设备同步状态与最近 ingest 事件。

### 响应示例

```json
{
  "devices": [
    {
      "device_id": "iphone-15-pro-max",
      "bundle_id": "com.example.myAppleHealthyBridge",
      "last_seen_at": "2026-03-31T16:31:00",
      "last_sent_at": "2026-03-31T16:30:58",
      "last_sync_at": "2026-03-31T16:31:00",
      "last_sync_status": "completed",
      "last_error_message": null,
      "last_items_count": 120,
      "last_accepted_count": 120,
      "last_deduplicated_count": 30,
      "updated_at": "2026-03-31T16:31:00",
      "anchor_count": 4,
      "anchors_updated_at": "2026-03-31T16:31:00"
    }
  ],
  "recent_events": [
    {
      "id": 18,
      "device_id": "iphone-15-pro-max",
      "bundle_id": "com.example.myAppleHealthyBridge",
      "sent_at": "2026-03-31T16:30:58",
      "received_at": "2026-03-31T16:31:00",
      "item_count": 120,
      "accepted_count": 120,
      "deduplicated_count": 30,
      "status": "completed",
      "error_message": null
    }
  ]
}
```

### 排查建议

如果客户端说“已经发了，但服务端没看到”，优先看：

- `devices[].last_seen_at`
- `devices[].last_sync_status`
- `devices[].last_error_message`
- `recent_events[0]`

## GET /api/device-sync-state/anchors

用于让 iOS 客户端按 `device_id` 读取服务端保存的 anchor map，在本地 anchor 丢失时恢复增量同步位置。

### 查询参数

- `device_id` 必填
- `bundle_id` 选填
  - 如果传了，就会额外校验该 `device_id` 当前登记的 bundle id 是否匹配

### 请求示例

```text
GET /api/device-sync-state/anchors?device_id=iphone-15-pro-max&bundle_id=com.example.myAppleHealthyBridge
```

### 响应示例

```json
{
  "device": {
    "device_id": "iphone-15-pro-max",
    "bundle_id": "com.example.myAppleHealthyBridge",
    "last_seen_at": "2026-03-31T16:31:00",
    "last_sent_at": "2026-03-31T16:30:58",
    "last_sync_at": "2026-03-31T16:31:00",
    "last_sync_status": "completed",
    "last_error_message": null,
    "last_items_count": 120,
    "last_accepted_count": 120,
    "last_deduplicated_count": 30,
    "updated_at": "2026-03-31T16:31:00",
    "anchor_count": 4,
    "anchors_updated_at": "2026-03-31T16:31:00"
  },
  "anchors": {
    "HKQuantityTypeIdentifierHeartRate": "base64-anchor",
    "HKCategoryTypeIdentifierSleepAnalysis": "base64-anchor"
  },
  "anchor_records": [
    {
      "record_type": "HKCategoryTypeIdentifierSleepAnalysis",
      "anchor_value": "base64-anchor",
      "updated_at": "2026-03-31T16:31:00"
    },
    {
      "record_type": "HKQuantityTypeIdentifierHeartRate",
      "anchor_value": "base64-anchor",
      "updated_at": "2026-03-31T16:31:00"
    }
  ]
}
```

### 失败响应

`404 Not Found`

```json
{
  "detail": "未找到该 device_id 的同步状态"
}
```

或：

```json
{
  "detail": "device_id 存在，但 bundle_id 不匹配"
}
```

## 客户端实现建议

- base URL 只保存根地址
- token 允许为空
- 每种类型单独维护 anchor
- 本地 anchor 缺失时，优先尝试读取 `/api/device-sync-state/anchors`
- payload 做成可重试、可重放
- 不要因为服务端已经幂等，就放弃本地去重和错误重试控制

## 当前建议优先同步的类型

- `HKQuantityTypeIdentifierHeartRate`
- `HKQuantityTypeIdentifierOxygenSaturation`
- `HKQuantityTypeIdentifierRespiratoryRate`
- `HKQuantityTypeIdentifierStepCount`
- `HKCategoryTypeIdentifierSleepAnalysis`

## 当前已知限制

- 只支持 `kind = sample`
- 还没有服务端分页式 ingest 查询接口
- 还没有“服务端告诉客户端应同步哪些 type”的配置接口
- workout 相关对象后续大概率需要单独 schema，而不是直接塞进当前 sample ingest

## 参考文件

- [backend/main.py](/programHost/vibe-coding/myAppleHealthy/backend/main.py)
- [backend/schema.sql](/programHost/vibe-coding/myAppleHealthy/backend/schema.sql)
- [backend/INCREMENTAL_SYNC.md](/programHost/vibe-coding/myAppleHealthy/backend/INCREMENTAL_SYNC.md)
