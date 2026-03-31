# 增量导入与告警方案

## 先说结论

如果你的数据源仍然是“苹果健康导出 XML”，那它更适合：

- 每天一次
- 每小时一次
- 手动触发后增量导入

它**不适合天然每 5 分钟一次**，因为苹果健康官方导出并不是一个面向高频实时同步的接口。

真正要做到接近实时，应该做的是：

1. 在 iPhone 侧做一个 HealthKit 采集桥
2. 用增量查询拿最近变化
3. 推送到你的后端
4. 后端落库后跑规则引擎

## 三种可行方案

### 方案 A：继续吃导出 XML

适合：

- 先把系统搭起来
- 历史数据全量导入
- 每天或每小时补一次

做法：

- 定期把新的 `导出.xml` 放到 [apple_health_export](/programHost/vibe-coding/myAppleHealthy/apple_health_export)
- 用 [run_incremental.sh](/programHost/vibe-coding/myAppleHealthy/backend/run_incremental.sh) 执行导入
- 依赖 `record_hash` / `workout_hash` 做幂等

优点：

- 最容易开始
- 不需要写 iPhone App

缺点：

- 不是真实时
- 每 5 分钟一份全量 XML 不现实

### 方案 B：iPhone 端桥接 App + HealthKit 增量同步

适合：

- 每 5 分钟以内同步
- 做健康告警
- 后面要长期自用

做法：

- iPhone 上做一个有 HealthKit 权限的 App
- 用 `HKObserverQuery` 监听健康数据变化
- 用 `enableBackgroundDelivery` 让系统在后台有机会唤醒
- 用 `HKAnchoredObjectQuery` 拉取自上次 anchor 以来的增量数据
- 把增量 JSON 推到你自己的后端 `/ingest`

优点：

- 这是最接近“官方正确姿势”的方案
- 真正支持增量
- 能做分钟级同步和告警

缺点：

- 需要开发 iOS App
- 后台触发不是绝对准点，iOS 仍然会调度

### 方案 C：第三方设备直连你的后端

适合：

- 你核心想监控的是心率、血氧、步数
- 有些设备本身就能提供云 API 或本地桥接

做法：

- 华为 / Zepp / 其他设备如果有开放能力，就不先经过 Apple Health
- 直接同步到你的后端
- 再把 Apple Health 当汇总层

优点：

- 某些指标可能更快
- 对告警链路更短

缺点：

- 来源会变复杂
- 各厂商接口不统一

## 我建议的落地顺序

### 第 1 阶段：先稳定历史库

- 用当前 importer 跑通 MySQL
- 前端先能看图
- 把导入和展示做稳定

### 第 2 阶段：做准实时同步

- 开发一个最小 iPhone 端桥接 App
- 优先同步：
  - 心率
  - 血氧
  - 呼吸频率
  - 睡眠状态变化
  - 训练开始/结束
- 后端新增：
  - `ingest_events`
  - `device_sync_state`
  - `alert_rules`
  - `alert_events`

### 第 3 阶段：做健康告警

第一版规则建议只做少量高价值规则：

- 静息心率持续高于阈值
- 血氧低于阈值并持续一段时间
- 睡眠不足连续多天
- 当天活动量极低
- 夜间心率异常高

## 每 5 分钟调度怎么做

### 如果还是 XML 导入

- Linux `cron`
- `systemd timer`
- 容器环境下用 `supercronic`

示例：

```cron
*/5 * * * * cd /programHost/vibe-coding/myAppleHealthy && HEALTH_DB_PASSWORD='你的密码' /programHost/vibe-coding/myAppleHealthy/backend/run_incremental.sh >> /tmp/apple_health_import.log 2>&1
```

但这只适用于“你已经有新的导出文件持续落到目录里”。

### 如果是 iPhone 桥接

- 手机侧：HealthKit 变化触发
- 服务端：收到数据后立即写库
- 服务端：写库后立即执行规则判断
- 告警：企业微信 / Telegram / 邮件 / 短信

## 告警系统最小架构

- `raw ingest`
- `normalized health_records`
- `rule evaluator`
- `alert_events`
- `notification dispatcher`

建议先做“服务端规则 + 企业微信机器人 / Telegram Bot”。
