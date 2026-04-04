# NEXT STEPS

本文件记录 `myAppleHealthy` 项目在 `2026-04-04` 这一轮结束时的真实状态，供下次继续开发时直接接手。

## 当前结论

项目方向不变，仍然是：

- `B 方案`
- iPhone 侧 HealthKit bridge App
- 增量同步到自建后端
- 服务端做健康告警

当前 B 方案服务端最小接入已经落地，dashboard 主干优化也已经完成。

这一轮还额外确认并修掉了多来源累计指标的重复累加问题，以及小时分布图的时间分桶偏斜问题。

## 这轮真正完成了什么

### 1. 后端从单文件拆成模块化结构

当前结构已经拆成：

- `backend/main.py`
- `backend/config.py`
- `backend/database.py`
- `backend/cache.py`
- `backend/responses.py`
- `backend/routes/*`
- `backend/services/*`
- `backend/queries/*`

`main.py` 现在只保留：

- FastAPI app 初始化
- CORS 中间件
- dashboard 静态目录挂载
- 路由注册
- startup schema ensure

### 2. dashboard 性能问题已做第一轮硬优化

已经完成：

- 数据库连接池
- TTL 缓存
- 首页查询合并
- 睡眠查询改写
- 关键索引补充
- 预聚合表 `system_summary`
- 预聚合表 `record_type_stats`

当前这些接口已经不再直接扫大表做全量聚合：

- `/api/stats/overview`
- `/api/records/types`

### 3. API 统一响应格式

当前 API 已逐步统一为：

```json
{
  "data": {},
  "meta": {
    "generated_at": "..."
  }
}
```

前端请求层已经兼容自动解包。

### 4. dashboard 前端已完成重构

前端已从单文件拆成：

- `frontend/index.html`
- `frontend/styles.css`
- `frontend/app.js`

已经落地：

- Hero 重构
- 信号卡片
- Chart.js 图表
- 深浅主题
- 骨架屏
- 移动端底部导航
- 详情弹窗

### 5. 新分析能力已落地

已新增并接入首页：

- `/api/sleep/quality`
- `/api/workouts/weekly-summary`
- `/api/records/by-source`

首页现在已经支持：

- 健康评分
- 睡眠质量摘要
- 训练周报摘要
- 主设备同步状态

### 6. 路线地图与热力图已落地

已新增并验证：

- `GET /api/workouts/routes`
- `GET /api/workouts/{id}/route`
- `GET /api/workouts/routes/heatmap`

dashboard 已接入 Leaflet 地图，能直接查看最近带 GPS 的训练路线，也支持查看常走热区。

当前数据库里真实已有：

- `140` 条路线
- `167,043` 个 `route_points`

### 7. 详情弹窗 bug 已修复

`今日步数` / `今日心率` 点开后的小时视图，之前存在两个问题：

- `hidden` 状态被样式覆盖，导致还会看到 `7天 / 30天`
- 快速切换时旧请求可能覆盖新请求

当前都已经修掉。

### 8. 小时分布口径已修正

已确认并修掉：

- `今日心率` 小时视图
- `今日步数` 小时视图
- 以及其他复用 `/api/records/hourly` 的小时柱状图

这次调整分成两类：

- 对心率等均值 / 瞬时指标，不再只按 `start_at` 分桶，改为按样本时间锚点归桶
- 对步数、能量、距离、爬楼、站立时间等累计型指标，不再把整条样本只落到开始小时，改为按样本覆盖时长拆分到各小时

当前口径下：

- `/api/records/hourly` 的 `sum` 类累计型指标，会先做单日优先来源解析，再做小时拆分
- `/api/records/hourly` 的 `avg/max/min/count` 类，以及 `/api/heart-rate?granularity=hourly`，会按时间锚点分桶

### 9. 自动化测试已补第一轮

当前已经新增 `backend/tests/`，并覆盖这些高价值回归点：

- dashboard 响应包装
- 多来源累计指标聚合口径
- 小时分布分桶口径
- records / energy 路由口径
- 运动路线接口采样
- ingest 去重统计
- ingest 失败时更新原事件而不是重复插入失败事件

### 10. 累计型指标口径已改成“优先来源解析”

已确认并修掉：

- `StepCount`
- `ActiveEnergyBurned`
- `BasalEnergyBurned`
- `DistanceWalkingRunning`
- `FlightsClimbed`
- 以及同类累计型距离 / 时间 / 次数指标

当前 dashboard / `/api/stats/today` / `/api/records/daily` / `/api/records/hourly` / `/api/energy` 不再直接把多设备来源简单相加。

当前规则：

- 先选单日总量更大的来源
- 平手时再偏向 Watch
- 小时分布里再按样本覆盖时长拆到各小时

## 当前验证结果

已实际通过的检查包括：

- `python3 -m compileall backend`
- `node --check frontend/app.js`
- `python3 -m pytest backend/tests`
- `GET /api/dashboard/home`
- `GET /api/sleep/quality`
- `GET /api/workouts/weekly-summary`
- `GET /api/workouts/routes`
- `GET /api/workouts/{id}/route`
- `GET /api/workouts/routes/heatmap`
- `GET /api/records/by-source`
- `GET /api/records/daily?type=HKQuantityTypeIdentifierDistanceWalkingRunning&agg=sum`
- `GET /api/records/hourly?type=HKQuantityTypeIdentifierStepCount&agg=sum`
- `GET /api/records/hourly?type=HKQuantityTypeIdentifierHeartRate&agg=avg`
- `GET /api/energy`
- `GET /dashboard/`

当前运行方式：

- 后端通过 Docker 启动
- importer 通过 Docker 单次任务运行
- 当前服务端端口仍是 `18000`

## 当前仍未完成的项

这轮结束后，真正还没做完的主要是：

1. `aiomysql` 异步化
2. `health_records` 表分区
3. `import_batches` 历史失败批次治理
4. 多设备对比完整版
   - 设备覆盖时间线
   - 同指标交叉验证
5. 非累计型指标的多来源偏斜治理
   - 心率 / 血氧 / 其他均值类指标
   - 不要把重复样本当独立来源一起平均
   - 当前只修了小时分桶，不等于多来源去偏已经完成
6. 训练周报完整分析页
   - 热力图
   - 类型分布图
7. `alert_rules`
8. `alert_events`
9. 正式 migration 体系（如 Alembic）
10. 前端更完整的自动化测试
   - 详情弹窗交互
   - 路线面板模式切换

## 下次优先做什么

建议顺序：

1. 训练周报完整分析页
2. 多设备历史来源对比页
3. 非累计型指标的多来源偏斜治理
4. `alert_rules` / `alert_events`
5. 前端交互自动化测试
6. 再考虑 `aiomysql` 和分区

## 下次不要重复踩的坑

- 不要把前端 API 地址写死成固定内网地址
- 不要把个人设备标识、内网 IP、数据库密码写进文档或示例
- 不要只看 `import_batches.status='running'` 就认定导入还活着
- dashboard 静态资源有浏览器缓存，前端改完要硬刷新
- 睡眠统计要注意 `AsleepUnspecified` 与 staged sleep 的重复计时问题
- 多来源累计指标不要直接 `SUM(value_num)`，至少要先做单日来源解析
- 累计型小时分布不要把整条样本直接归到 `start_at` 所在小时
- 均值 / 瞬时类小时分布不要默认只按 `start_at` 分桶
- modal 详情切换要注意异步竞态，旧请求不能覆盖新状态
- 地图面板要注意模式切换竞态，热力图请求和单路线请求不能互相覆盖

## 重要命令

### 启动后端

```bash
cd /programHost/vibe-coding/myAppleHealthy

export HEALTH_DB_PASSWORD='your-mysql-password'

docker compose up -d --build backend
```

### 启动导入器

```bash
cd /programHost/vibe-coding/myAppleHealthy

export HEALTH_DB_PASSWORD='your-mysql-password'

docker compose run --rm importer
```

### 检查导入状态

```bash
curl http://127.0.0.1:18000/api/import-status
```

### 查看后端日志

```bash
docker compose logs -f backend
```
