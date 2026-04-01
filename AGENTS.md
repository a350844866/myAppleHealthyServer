# AGENTS

这个文件是 `myAppleHealthy` 项目的代理接手说明。进入项目后应先阅读此文件，再结合 [NEXT_STEPS.md](/programHost/vibe-coding/myAppleHealthy/NEXT_STEPS.md) 继续工作。

## 项目目标

这是一个自建的个人健康管理系统。

当前数据来源：

- Apple Health 历史导出
- ECG CSV
- GPX 轨迹文件

长期目标：

- 不再依赖手动导出
- 改走 `B 方案`
- iPhone 侧 HealthKit bridge App 增量同步到自建后端
- 服务端做健康告警

## 当前技术栈

### 后端

- Python
- FastAPI
- PyMySQL
- MySQL 8

### 前端

- 轻量静态 dashboard
- 由 FastAPI 同源托管在 `/dashboard/`
- 当前文件：
  - [frontend/index.html](/programHost/vibe-coding/myAppleHealthy/frontend/index.html)
  - [frontend/styles.css](/programHost/vibe-coding/myAppleHealthy/frontend/styles.css)
  - [frontend/app.js](/programHost/vibe-coding/myAppleHealthy/frontend/app.js)

## 数据库与端口

数据库连接通过环境变量提供，不要把真实密码写回仓库。

默认约定：

- `HEALTH_DB_HOST=127.0.0.1`
- `HEALTH_DB_PORT=3306`
- `HEALTH_DB_USER=root`
- `HEALTH_DB_NAME=apple_health`

服务端端口：

- `18000`

不要默认使用：

- `8000`

## 当前关键文件

- [README.md](/programHost/vibe-coding/myAppleHealthy/README.md)
- [NEXT_STEPS.md](/programHost/vibe-coding/myAppleHealthy/NEXT_STEPS.md)
- [backend/INCREMENTAL_SYNC.md](/programHost/vibe-coding/myAppleHealthy/backend/INCREMENTAL_SYNC.md)
- [backend/schema.sql](/programHost/vibe-coding/myAppleHealthy/backend/schema.sql)
- [backend/importer.py](/programHost/vibe-coding/myAppleHealthy/backend/importer.py)
- [backend/main.py](/programHost/vibe-coding/myAppleHealthy/backend/main.py)

## 当前已知事实

- SQLite 版本已经放弃，当前代码以 MySQL 为准
- dashboard 默认已改成同源请求，不再使用固定 `127.0.0.1:8000`
- B 方案服务端最小接入已经落地
- dashboard 主干优化已经完成
- 当前已经有路线地图和热力图
- 当前已经有一组后端自动化回归测试

## 当前主要未完成项

1. `aiomysql` 异步化
2. `health_records` 表分区
3. `import_batches` 历史治理
4. 多设备对比完整版
5. 训练周报完整分析页
6. `alert_rules`
7. `alert_events`
8. 前端交互自动化测试

## 启动命令

### 启动后端

```bash
cd /programHost/vibe-coding/myAppleHealthy

export HEALTH_DB_PASSWORD='your-mysql-password'

docker compose up -d --build backend
```

### 运行导入器

```bash
cd /programHost/vibe-coding/myAppleHealthy

export HEALTH_DB_PASSWORD='your-mysql-password'

docker compose run --rm importer
```

说明：

- `importer` 是一次性任务容器
- 导入完成后应自动退出
- 不要把 importer 作为常驻服务使用

## 对接手代理的要求

- 不要假设 `8000` 可用
- 不要把前端 API 地址写死成固定内网地址
- 不要重新引回 SQLite
- 不要把项目方向改回“人工手动导出”
- 不要把真实密码、内网地址、个人设备标识提交到仓库
- 修改前请优先阅读 [NEXT_STEPS.md](/programHost/vibe-coding/myAppleHealthy/NEXT_STEPS.md)
