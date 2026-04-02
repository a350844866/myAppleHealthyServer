# myAppleHealthyServer

`myAppleHealthyServer` 是一个自建的 Apple Health 服务端，当前包含：

- FastAPI 后端
- MySQL 存储
- Apple Health 历史导出导入器
- iOS bridge 增量同步接口
- 同源静态 dashboard

## 当前能力

当前已经落地并可用的核心能力：

- `POST /ingest`
- `GET /api/device-sync-state`
- `GET /api/device-sync-state/anchors`
- `GET /api/import-status`
- `GET /api/dashboard/home`
- `GET /api/sleep/quality`
- `GET /api/workouts/weekly-summary`
- `GET /api/workouts/routes`
- `GET /api/workouts/{id}/route`
- `GET /api/workouts/routes/heatmap`

dashboard 已完成这一轮优化：

- 前端拆分为 `index.html + styles.css + app.js`
- Chart.js 图表、深浅主题、骨架屏
- 预聚合表 `system_summary` / `record_type_stats`
- 统一 API 响应 `{data, meta}`
- 累计型指标已改成按优先来源解析，避免多设备重复累加
- 首页健康评分、睡眠质量、训练周报
- 最近运动路线地图
- 路线热力图
- 后端自动化回归测试

## 仓库结构

- [backend](./backend)
  - FastAPI 服务、数据库脚本、导入器、接口文档
- [frontend](./frontend)
  - 同源 dashboard 前端
- [docker-compose.yml](./docker-compose.yml)
  - 本地或服务器部署入口
- [NEXT_STEPS.md](./NEXT_STEPS.md)
  - 当前状态与后续待办

## 快速启动

先准备好 MySQL，并设置至少这些环境变量：

- `HEALTH_DB_HOST`
- `HEALTH_DB_PORT`
- `HEALTH_DB_USER`
- `HEALTH_DB_PASSWORD`
- `HEALTH_DB_NAME`

可选变量：

- `HEALTH_ALLOWED_ORIGINS`
- `INGEST_API_TOKEN`
- `HEALTH_LOCAL_TZ`
- `IMPORT_STALE_SECONDS`
- `OPENROUTER_API_KEY`
- `OPENROUTER_MODEL`
- `OPENROUTER_ALLOWED_MODELS`

启动后端：

```bash
export HEALTH_DB_PASSWORD='your-mysql-password'
docker compose up -d --build backend
```

默认访问地址：

```text
http://127.0.0.1:18000
```

查看日志：

```bash
docker compose logs -f backend
```

运行导入器：

```bash
export HEALTH_DB_PASSWORD='your-mysql-password'
docker compose run --rm importer
```

## 自动化测试

当前已经补了一组后端回归测试，覆盖：

- dashboard 响应包装
- 多来源累计指标聚合口径
- records / energy 路由口径
- 运动路线接口采样
- ingest 去重统计与失败更新逻辑

运行方式：

```bash
python3 -m pip install -r backend/requirements.txt
python3 -m pytest backend/tests
```

## iOS 客户端联调

iOS 客户端应把服务端根地址配置为：

```text
http://your-server-host:18000
```

不要把 `/ingest` 直接写进 Base URL。

客户端当前联调重点：

- 服务端游标恢复
- `POST /ingest` 增量同步
- `GET /api/records/recent` 联调排查

## 文档入口

- [backend/README.md](./backend/README.md)
- [backend/IOS_CLIENT_API.md](./backend/IOS_CLIENT_API.md)
- [backend/INCREMENTAL_SYNC.md](./backend/INCREMENTAL_SYNC.md)
- [AGENTS.md](./AGENTS.md)
- [NEXT_STEPS.md](./NEXT_STEPS.md)

## 当前未完成项

还没做完的主要是：

- `aiomysql` 异步化
- `health_records` 分区
- `import_batches` 历史治理
- 多设备对比分析完整版
- 非累计型指标的多来源偏斜治理
- 训练周报完整分析页
- `alert_rules` / `alert_events`
