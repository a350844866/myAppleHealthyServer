# iOS App Handoff

这份文档给 iOS 端开发使用，目标不是重新设计后端，而是让客户端基于当前已经落地的服务端能力直接开工。

## 当前真实状态

已经完成：

- `POST /ingest`
- `GET /api/device-sync-state`
- 服务端可选 Bearer Token 鉴权
- 样本幂等写入 `health_records`
- 同步状态写入 `device_sync_state`
- anchor 写入 `device_sync_anchors`

还没完成：

- 告警规则接口
- workout 专用 ingest
- 服务端配置下发

所以 iOS 侧现在应该做的是：

1. HealthKit 权限
2. Anchored query
3. 本地 anchor 持久化
4. 统一 JSON payload
5. `/ingest` 上传
6. 简单设置页和手动同步

## 客户端对接重点

- Base URL 填服务端根地址，例如 `http://your-server-host:18000`
- 客户端请求的是 `POST <Base URL>/ingest`
- 如果服务端没有启用 `INGEST_API_TOKEN`，`Authorization` 可以留空
- 如果启用了 token，必须带 `Authorization: Bearer <token>`

## 当前 payload 形状

```json
{
  "device_id": "iphone-xxx",
  "bundle_id": "com.example.myAppleHealthyBridge",
  "sent_at": "2026-03-31T08:30:00Z",
  "items": [
    {
      "source": "healthkit",
      "kind": "sample",
      "type": "HKQuantityTypeIdentifierHeartRate",
      "uuid": "sample-uuid",
      "start_at": "2026-03-31T08:20:00Z",
      "end_at": "2026-03-31T08:20:00Z",
      "value": 72,
      "unit": "count/min",
      "metadata": {
        "source_name": "Apple Watch",
        "source_version": "11.4"
      }
    }
  ],
  "anchors": {
    "HKQuantityTypeIdentifierHeartRate": "base64-anchor"
  }
}
```

限制：

- 当前只支持 `kind = sample`
- 不要先发 workout/event/route

## 第一阶段优先类型

- `HKQuantityTypeIdentifierHeartRate`
- `HKQuantityTypeIdentifierOxygenSaturation`
- `HKQuantityTypeIdentifierRespiratoryRate`
- `HKQuantityTypeIdentifierStepCount`
- `HKCategoryTypeIdentifierSleepAnalysis`

## 最低可接受结构

- `HealthKitManager`
- `SyncStore`
- `IngestClient`
- `SyncCoordinator`
- 一个极简设置页

## 最小验收标准

- 能请求 HealthKit 权限
- 能拉到至少一种样本
- 能保存并复用 anchor
- 能编码成统一 JSON payload
- 能把数据发到可配置服务端地址
- 能显示最近一次同步成功或失败

## 客户端必读

详细接口契约看：

- [backend/IOS_CLIENT_API.md](/programHost/vibe-coding/myAppleHealthy/backend/IOS_CLIENT_API.md)
- [backend/INCREMENTAL_SYNC.md](/programHost/vibe-coding/myAppleHealthy/backend/INCREMENTAL_SYNC.md)
- [backend/main.py](/programHost/vibe-coding/myAppleHealthy/backend/main.py)
