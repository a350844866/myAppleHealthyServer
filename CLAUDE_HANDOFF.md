# Claude Handoff

这份文档给后续接手 `myAppleHealthyServer` 的 Claude Code / Codex。长期说明看 [README.md](./README.md)，这里聚焦当前真实可用能力和开发注意事项。

## 当前状态

- 服务端已经上线并在用
- iOS 客户端 `myAppleHealthyBridge` 已经接通当前接口
- 设备同步状态、服务端游标恢复、最近同步明细查询都已落地
- 当前主要关注点是保持接口稳定，不要随手破坏既有客户端契约

## 当前关键接口

- `POST /ingest`
- `GET /api/device-sync-state`
- `GET /api/device-sync-state/anchors`
- `GET /api/records/recent`

实现文件：

- [backend/main.py](./backend/main.py)

详细契约：

- [backend/IOS_CLIENT_API.md](./backend/IOS_CLIENT_API.md)

## 当前数据流

`POST /ingest` 当前会做三件事：

1. 记录 ingest 批次
2. 把样本幂等写入 `health_records`
3. 维护 `device_sync_state` 和 `device_sync_anchors`

客户端当前依赖这些行为：

- 服务端可返回设备级 anchors 给客户端恢复
- 同一批样本重传时必须幂等去重
- 最近明细可以按 `device_id` 拉取

## 数据库与部署

- 数据库：MySQL
- 默认服务端口：`18000`
- Docker 启动入口：`docker-compose.yml`
- 后端详细启动说明见 [backend/README.md](./backend/README.md)

## 关键表

- `health_records`
- `ingest_events`
- `device_sync_state`
- `device_sync_anchors`

## 当前开发注意事项

- 不要轻易修改 `/ingest` 的请求体结构
- 不要改变 `device_id + bundle_id + type + uuid` 这套幂等语义
- 新增接口时，优先做调试/观察能力，再考虑复杂写接口
- 如果要改动客户端相关契约，必须同步更新 `backend/IOS_CLIENT_API.md`
- 默认以“改动已推到远程仓库”为结束标准，不停在本地 commit

## 当前已知未完成项

- workout 专用 ingest
- 删除样本 / 回滚同步接口
- 服务端配置下发
- 更完整的后台补偿与运维观测

## 常用命令

启动后端：

```bash
export HEALTH_DB_PASSWORD='你的 MySQL 密码'
docker compose up -d --build backend
```

查看日志：

```bash
docker compose logs -f backend
```

语法检查：

```bash
python3 -m py_compile backend/main.py
```

## 关联仓库

iOS 客户端仓库在：

- `/Users/liulin/programHost/vibe-coding/appleHealthIosClient`

如果服务端改了客户端相关契约，必须同时检查客户端这些文件：

- `/Users/liulin/programHost/vibe-coding/appleHealthIosClient/myAppleHealthyBridge/IngestClient.swift`
- `/Users/liulin/programHost/vibe-coding/appleHealthIosClient/myAppleHealthyBridge/SyncCoordinator.swift`
- `/Users/liulin/programHost/vibe-coding/appleHealthIosClient/myAppleHealthyBridge/RecentSyncedDataView.swift`
