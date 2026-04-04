# Apple Health Backend

这个版本以 MySQL 8 为目标，不再使用 SQLite。

## 默认约定

- MySQL Host: `127.0.0.1`
- MySQL Port: `3306`
- MySQL User: `root`
- 数据库名默认: `apple_health`
- 导出目录默认: 仓库根目录下的 [apple_health_export](/programHost/vibe-coding/myAppleHealthy/apple_health_export)

## 首次启动

### 后端用 Docker 运行

在仓库根目录执行：

```bash
cd /programHost/vibe-coding/myAppleHealthy

export HEALTH_DB_PASSWORD='你的 MySQL 密码'

docker compose up -d --build backend
```

默认约定：

- 对外端口：`18000`
- 容器内 API：`http://0.0.0.0:18000`
- 宿主访问：`http://127.0.0.1:18000/docs`
- `docker-compose.yml` 默认把 `HEALTH_DB_HOST` 设为 `host.docker.internal`
- Linux 通过 `extra_hosts: host-gateway` 回连宿主机 MySQL

如需查看日志：

```bash
docker compose logs -f backend
```

### importer 用 Docker 单次运行

```bash
cd /programHost/vibe-coding/myAppleHealthy

export HEALTH_DB_PASSWORD='你的 MySQL 密码'

docker compose run --rm importer
```

文档地址: `http://127.0.0.1:18000/docs`
仪表盘地址: `http://127.0.0.1:18000/dashboard/`

说明：

- `importer` 是一次性任务容器，不是常驻服务
- 导入完成后会自动退出，不会一直挂着
- 进度逻辑不需要重写，仍然复用 MySQL 中的 `import_batches` / `import_files` / `last_progress_at`
- 如果中断后重跑，仍然走现有的残留 `running` 清理和断点恢复逻辑

### AI 首页分析

首页现在支持可选的 OpenRouter AI 分析卡片。

需要这些环境变量：

- `OPENROUTER_API_KEY`
- `OPENROUTER_MODEL`
  - 默认：`anthropic/claude-sonnet-4.6`
- `OPENROUTER_ALLOWED_MODELS`
  - 默认：`anthropic/claude-sonnet-4.6,minimax/minimax-m2.7`

可选：

- `OPENROUTER_APP_NAME`
- `OPENROUTER_SITE_URL`
- `AI_ANALYSIS_CACHE_TTL_SECONDS`

说明：

- AI 分析不会在首页自动调用
- 需要用户手动点击“生成分析”
- 后端会对同一份近期数据做短时缓存，避免重复消耗 token
- 成功生成的首页 AI 报告会写入 `ai_dashboard_reports`

### API 响应格式

当前接口统一使用：

```json
{
  "data": {},
  "meta": {
    "generated_at": "2026-04-01T12:00:00+08:00"
  }
}
```

列表接口会在 `meta` 里额外带上 `total`、`limit`、`offset` 等信息。

## 导入说明

- `python backend/importer.py`
  按文件版本做增量导入
- `python backend/importer.py --force`
  强制重扫所有文件，依赖唯一 hash 保证幂等
- `python backend/importer.py --xml-only`
- `python backend/importer.py --gpx-only`
- `python backend/importer.py --ecg-only`

也可以直接映射成容器命令：

- `docker compose run --rm importer --force`
- `docker compose run --rm importer --xml-only`
- `docker compose run --rm importer --gpx-only`
- `docker compose run --rm importer --ecg-only`

## 主要表

- `profile`
- `import_batches`
- `import_files`
- `health_records`
- `workouts`
- `workout_statistics`
- `workout_events`
- `workout_routes`
- `route_points`
- `activity_summaries`
- `ecg_readings`
- `ingest_events`
- `device_sync_state`
- `device_sync_anchors`

## 设计要点

- `health_records` 保留原始 HealthKit 事件流
- 通过 `record_hash / workout_hash / statistic_hash / event_hash / ecg_hash` 做幂等去重
- `workout_routes` 与 `route_points` 分开，便于后面做地图与轨迹分析
- `activity_summaries` 单独存每日三环，避免每次都从原始记录重算
- API 和 importer 都直接连接 MySQL，不再依赖 SQLite 方言
- 对步数、活动能量、基础代谢、步行距离、爬楼等累计型指标，dashboard 和统计接口会按单日优先来源解析，不直接把多设备来源简单相加
- 对累计型指标的小时分布，服务端会先选单日优先来源，再按样本覆盖时长拆分到各小时，而不是整条记录只落到 `start_at` 所在小时
- 对心率这类均值 / 瞬时指标的小时分布，服务端会按样本时间锚点分桶，避免长样本仅按开始时间归桶

## 增量同步

参考 [INCREMENTAL_SYNC.md](/programHost/vibe-coding/myAppleHealthy/backend/INCREMENTAL_SYNC.md)。

当前 bridge 端最小可用接口：

- `POST /ingest`
  - 兼容 `myAppleHealthyBridge` 当前 payload
  - 支持可选 `Authorization: Bearer <token>`，通过 `INGEST_API_TOKEN` 控制
  - 幂等去重写入 `health_records`
- `GET /api/device-sync-state`
  - 查看最近设备同步状态与 ingest 事件

dashboard 相关新增能力：

- `GET /api/dashboard/home`
- `GET /api/sleep/quality`
- `GET /api/workouts/weekly-summary`
- `GET /api/workouts/routes`
- `GET /api/workouts/{id}/route`
- `GET /api/workouts/routes/heatmap`

## 自动化测试

当前已经补了一组后端回归测试，运行方式：

```bash
python3 -m pip install -r backend/requirements.txt
python3 -m pytest backend/tests
```

当前已覆盖的高价值点包括：

- dashboard 响应包装
- 多来源累计指标聚合口径
- 小时分布分桶口径
- records / energy 路由口径
- 运动路线接口采样
- ingest 去重统计与失败更新逻辑
