# 短视频转码副本伸缩模拟器

FastAPI 模拟 Kubernetes 转码服务的副本伸缩，状态存 SQLite。监控漏报时按已知积压判断扩容；
缩容需连续两轮报齐且均建议缩容。在 `/docs`（Swagger UI）上报和查询。

## 规则实现

| 规则 | 实现 |
| --- | --- |
| 每 10 个待转 1 副本，向上取整，至少 1 个 | `app/scaler.py` 中 `_suggest()` |
| 漏报扩容：已知积压 > 目标×10 即扩，不等报齐 | `submit_report()` 内部分积压检查 |
| 缩容：就绪数==目标，且轮号连续两轮报齐、均建议缩容，取两轮建议较大值 | `submit_report()` 报齐分支 |
| 名单或目标变更后重新累计 | 名单更新 / 目标变更时清空连续低负载累计 |
| 过期名单、重报、迟到旧轮（含并发）无副作用 | `BEGIN IMMEDIATE` 事务；`UNIQUE(version, round, pod)` 兜底；忽略时不写库 |

## 接口（详见 /docs）

- `POST /roster` — 值班员更新就绪 Pod 名单（内容变化才升版号并清零累计）
- `POST /reports` — 监控上报 `{roster_version, round, pod, pending}`；过期/重报/迟到返回 `outcome=ignored`
- `GET /target` — 目标副本数及最近一次调整原因
- `GET /state` — 当前轮上报明细、低负载累计
- `GET /events` — 伸缩与名单变更历史
- `GET /health` — 实例就绪、所用 DB 路径、当前名单版号
- `GET /roster`、`POST /admin/reset`（演示/测试辅助）

## 启动

```bash
docker compose up --build
# scaler-a: http://localhost:8000/docs   scaler-b: http://localhost:8001/docs
```

compose 默认起**两个实例共享同一个 `scaler-data` 卷**，用于验证并发首次启动与接力服务。
只想要一个实例：`docker compose up --build scaler-a`。

本地开发：`pip install -r requirements.txt && uvicorn app.main:app --reload`
（DB 路径用环境变量 `SCALER_DB` 指定，默认 `./scaler.db`，compose 中为 `/data/scaler.db`）

测试：

```bash
python3 tests/scenario_test.py [base_url]   # 伸缩规则端到端，仅标准库
python3 tests/startup_test.py               # 并发启动 / 中断恢复 / 新实例接续，仅标准库
```

## 多实例共享 SQLite 卷

两个容器同时用同一个空卷启动时，旧代码有两处会失败：

1. `PRAGMA journal_mode=WAL` 在另一个进程持有写锁时会立刻报 `database is locked`；
2. 初始化是“先查 `state` 是否为空，再插入”，两个进程同时判断为空就都插入版号 0，
   后到者撞 `UNIQUE constraint failed: rosters.version`。
   若首次启动在“写完初始名单、未写 state”时退出，原卷再次启动也会因同样的重复主键失败。

修复（`app/scaler.py`）：

- 初始化改为 `BEGIN IMMEDIATE` 先序列化库文件创建，再做 WAL 切换；WAL 是数据库级持久设置，
  请求路径不再重复切换；连接统一设 `busy_timeout=30s`。
- 建表全部 `IF NOT EXISTS`，种子数据改为 `INSERT OR IGNORE`（版号 0 空名单 + 目标 1），
  整个初始化幂等；只留下部分表的卷、只剩初始名单的卷、零字节文件都能原地补齐。
- 初始化遇到 `locked/busy` 时短暂退避重试（最多 60 次，总时长受 busy_timeout 约束）。
- 所有写操作仍在 `BEGIN IMMEDIATE` 事务内，跨进程由 SQLite 写锁串行化；
  过期名单/重报/迟到旧轮依旧靠事务内校验 + `UNIQUE(version, round, pod)` 兜底。

## 实测结果

环境：本沙箱 Python 3.11 + SQLite 3.40.1，无 Docker，用两个 uvicorn 进程共享同一 DB 文件模拟两个容器。

1. `tests/scenario_test.py`：32 项断言全部通过（规则未改动，单实例与双实例共享卷各跑一遍）。
2. `tests/startup_test.py`：30 项断言全部通过。
   - 两个实例同时用空卷启动：都就绪，两实例查到的名单均为 `{"roster_version":0,"pods":[]}`、目标均为 1，`/docs` 均 200。
   - 中断卷恢复：零字节文件、只有空表、只有初始名单（旧代码必失败）三种卷都能启动，名单空、目标 1。
   - 演练接续：原实例已扩到目标 3 后启动新实例，启动期间原实例持续受理上报且**零失败**；
     新实例就绪后名单版号、目标、当前轮、缩容累计、事件流与原实例完全一致，并在新实例上继续受理同一轮上报。
   - 强杀恢复：8 次在首次启动窗口内 `kill -9`，原卷重启均成功，`PRAGMA integrity_check=ok`。
3. 压力复跑：60 轮两进程并发初始化、10 轮双 uvicorn 空卷同启、200 次跨实例并发上报，均无异常。
4. 两个实例日志无 error/warning。

## 改动清单

- `app/scaler.py` — 幂等、可并发、可中断恢复的初始化；连接统一 busy_timeout；伸缩规则不变
- `app/main.py` — 新增 `GET /health`；`/docs` 说明当前 DB 路径与多实例提示
- `docker-compose.yml` — 两个实例共享 `scaler-data` 卷（8000 / 8001），带健康检查
- `tests/startup_test.py` — 并发启动 / 中断恢复 / 新实例接续测试
- `README.md` — 本文件

## 余项

- 本沙箱无 Docker，`Dockerfile`/`docker-compose.yml` 未实际构建，仅做 YAML 结构与端口/卷校验；
  建议在有 Docker 的环境跑一次 `docker compose up --build` 冒烟。
- 扩容只按“当前轮已知积压”触发，不跨轮累计积压；如需按多轮未消化积压扩容需另加规则。
- 名单更新按内容集合判等（忽略顺序），重复提交相同名单不升版号、不清累计。
- 上报要求 `round >= 1`、`pending >= 0`；更旧的未报齐轮在新轮到达后即放弃，不再补算。
- 多实例共享的是**本地卷**（同一主机文件系统）。跨主机共享需要换成网络文件系统或真正的数据库，
  SQLite 的文件锁在网络文件系统上不可靠。
- 两个实例都暴露了 `POST /admin/reset`，生产化时应只在调试实例开启。
