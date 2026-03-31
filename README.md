# myAppleHealthyServer

`myAppleHealthyServer` 是 Apple Health 数据的服务端仓库，当前包含：

- FastAPI 后端
- MySQL 存储
- Health 导出文件导入器
- iOS 客户端增量同步接口

当前已经和 `myAppleHealthyBridge` 客户端联通的核心接口有：

- `POST /ingest`
- `GET /api/device-sync-state`
- `GET /api/device-sync-state/anchors`
- `GET /api/records/recent`

## 仓库结构

- [backend](./backend)
  - FastAPI 服务、数据库脚本、接口文档
- [frontend](./frontend)
  - 前端相关代码
- [docker-compose.yml](./docker-compose.yml)
  - 本地或服务器部署入口

## 快速启动

先准备好 MySQL，并设置至少这几个环境变量：

- `HEALTH_DB_HOST`
- `HEALTH_DB_PORT`
- `HEALTH_DB_USER`
- `HEALTH_DB_PASSWORD`
- `HEALTH_DB_NAME`

可选变量：

- `INGEST_API_TOKEN`
- `HEALTH_LOCAL_TZ`
- `IMPORT_STALE_SECONDS`

启动后端：

```bash
export HEALTH_DB_PASSWORD='你的 MySQL 密码'
docker compose up -d --build backend
```

默认对外端口：

```text
http://127.0.0.1:18000
```

查看日志：

```bash
docker compose logs -f backend
```

运行导入器：

```bash
export HEALTH_DB_PASSWORD='你的 MySQL 密码'
docker compose run --rm importer
```

## iOS 客户端联调

iOS 客户端应把服务端根地址配置为：

```text
http://your-server-host:18000
```

而不是把 `/ingest` 直接写进 Base URL。

客户端当前联调重点：

- 首次优先恢复服务端游标
- 服务端无游标时由客户端 `Start From Now` 建立基线
- 增量数据走 `POST /ingest`
- 最近同步明细通过 `GET /api/records/recent` 排查

## 文档入口

- [backend/README.md](./backend/README.md)
  - 后端启动、导入、表结构、设计说明
- [backend/IOS_CLIENT_API.md](./backend/IOS_CLIENT_API.md)
  - iOS 客户端当前接口契约
- [backend/INCREMENTAL_SYNC.md](./backend/INCREMENTAL_SYNC.md)
  - 增量同步思路与数据流
- [CLAUDE_HANDOFF.md](./CLAUDE_HANDOFF.md)
  - 给 Claude Code / Codex 的执行层交接说明
- [IOS_APP_HANDOFF.md](./IOS_APP_HANDOFF.md)
  - 给 iOS 端的阶段性说明

## 当前状态

当前这套服务端已经支持：

- 样本幂等写入 `health_records`
- 记录每次 ingest 批次
- 维护设备级同步状态
- 保存并恢复服务端 anchors
- 按 `device_id` 查看最近落库明细

当前还没有完成：

- workout 专用 ingest
- 删除样本 / 回滚同步接口
- 完整的服务端配置下发能力
