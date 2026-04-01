# NEXT STEPS

本文件记录 `myAppleHealthy` 项目在 `2026-04-01` 这一轮结束时的真实状态，供下次继续开发时直接接手。

## 当前结论

项目方向不变，仍然是：

- `B 方案`
- iPhone 侧 HealthKit bridge App
- 增量同步到自建后端
- 服务端做健康告警

当前 B 方案服务端最小接入已经落地，dashboard 主干优化也已经完成。

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

### 6. 路线地图已落地基础版

已新增并验证：

- `GET /api/workouts/routes`
- `GET /api/workouts/{id}/route`

dashboard 已接入 Leaflet 地图，能直接查看最近带 GPS 的训练路线。

当前数据库里真实已有：

- `140` 条路线
- `167,043` 个 `route_points`

### 7. 详情弹窗 bug 已修复

`今日步数` / `今日心率` 点开后的小时视图，之前存在两个问题：

- `hidden` 状态被样式覆盖，导致还会看到 `7天 / 30天`
- 快速切换时旧请求可能覆盖新请求

当前都已经修掉。

## 当前验证结果

已实际通过的检查包括：

- `python3 -m compileall backend`
- `node --check frontend/app.js`
- `GET /api/dashboard/home`
- `GET /api/sleep/quality`
- `GET /api/workouts/weekly-summary`
- `GET /api/workouts/routes`
- `GET /api/workouts/{id}/route`
- `GET /api/records/by-source`
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
5. 路线热力图
6. 训练周报完整分析页
   - 热力图
   - 类型分布图
7. `alert_rules`
8. `alert_events`
9. 正式 migration 体系（如 Alembic）

## 下次优先做什么

建议顺序：

1. 路线热力图
2. 训练周报完整分析页
3. 多设备历史来源对比页
4. `alert_rules` / `alert_events`
5. 再考虑 `aiomysql` 和分区

## 下次不要重复踩的坑

- 不要把前端 API 地址写死成固定内网地址
- 不要把个人设备标识、内网 IP、数据库密码写进文档或示例
- 不要只看 `import_batches.status='running'` 就认定导入还活着
- dashboard 静态资源有浏览器缓存，前端改完要硬刷新
- 睡眠统计要注意 `AsleepUnspecified` 与 staged sleep 的重复计时问题
- modal 详情切换要注意异步竞态，旧请求不能覆盖新状态

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
