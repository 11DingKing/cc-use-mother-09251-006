# 充电需求预测版本管理

面向交通分析团队与地方执行人员的**纯服务端**版本管理系统。节前连续修订期间，
每一次车流/充电需求预测所依据的原始数据、参数、区域映射与计算结果都以
**不可变版本**保存；执行端拿到的任何数字都可以回答三个问题：

1. 它是在什么假设下、由哪些输入版本算出来的？
2. 上游数据后来有没有被更正？这版数字是否受影响？
3. 被采用的预测对应哪份移动设施调度决策？谁在什么时候签署/撤销的？

仅依赖 Python 3.11 标准库（`sqlite3` + `http.server`），无第三方运行时依赖。

## 核心概念

| 概念 | 说明 |
|---|---|
| **不可变对象** | 原始批次/参数/区域映射/预测/决策序列化为规范化 JSON，以 SHA-256 内容哈希寻址，写入后永不修改；同内容写入幂等返回同一 ID |
| **版本 (version)** | 对象的登记记录，顺序编号 `v000001…`，携带来源键、创建人、时间与“更正自哪一版”指针 |
| **情景 (scenario)** | 一条只追加的**提交链**（root → add/replace/set 参数/set 映射）。沿链解析出某提交生效的输入集合、参数、映射 |
| **预测 (forecast)** | 在情景某提交上，由输入+参数+映射+引擎版本确定性算出，带内容指纹；同内容预测全局唯一 |
| **更正 (correction)** | 原始数据有误时登记**新版本**（`corrects` 指向旧版），历史对象原封不动；引用旧版的所有历史预测被追加 `affected` 失效标记 |
| **核销 (resolution)** | 情景替换为更正版本并重算后，旧预测上的标记变为 `resolved`，指向新预测版本 |
| **签署/撤销** | 追加式 adoption 事件；调度决策本身也是不可变对象，与预测版本绑定。撤销不删除历史，可再次采用 |

### 为什么执行端不会再拿到“对不上”的数字

* 预测内容指纹 = `hash(引擎版本, 参数对象哈希, 映射对象哈希, 各数据源对象哈希)`，
  假设不同则指纹必然不同，预测版本必然不同；
* 每次预测保存对**具体输入版本**的引用（`forecast_inputs`），更正沿该表精确传播到下游预测；
* 谱系核验（`verify`）会重新读取全部输入对象、校验哈希并用登记数据**精确重算**，
  与存储结果逐字节比对。

## 目录结构

```
service_09251_006/
├── clock.py               # 可替换的时间/ID 端口（冻结时钟、序列 ID 便于复现与测试）
├── hashing.py             # 规范化 JSON 与内容寻址
├── models.py              # 原始批次/参数/映射/决策的结构校验
├── forecast.py            # 确定性预测引擎：跨日聚合、峰值、置信区间、区域加权
├── repository.py          # 元数据 + 对象存储的查询仓储
├── container.py           # 应用装配（数据目录在源码目录之外）
├── cli.py                 # 离线管理命令
├── storage/
│   ├── object_store.py    # WORM 内容寻址对象存储（原子落盘、读时校验）
│   └── database.py        # SQLite 元数据库（提交链/作业/失效/签署事件）
├── services/
│   ├── snapshots.py       # 沿提交链解析情景快照
│   ├── versioning.py      # 导入/更正/派生/签署/比较/谱系核验
│   └── forecasting.py     # 预测作业：租约 + 分服务区检查点，崩溃可续跑
└── api/http_app.py        # HTTP API（标准库 ThreadingHTTPServer）
```

运行数据默认写入 `$SERVICE_09251_DATA_DIR`
（未设置时为 `~/.local/share/service_09251_006`），绝不写入源码目录：

```
<data-dir>/
├── meta.db                # 元数据（WAL 模式）
└── objects/{rb,pa,rm,fc,dc}/<hash前2位>/<kind>-<sha256>.json
```

## 预测方法（可解释）

1. 观测按 **UTC 日 × 日内时间桶**归类，跨多日聚合，得到每个桶的样本；
2. 桶预测值 = 样本均值 × `growth_factor`（车流再乘以 `traffic_conversion`，默认 0.1）；
   不确定度用样本标准误，缺样本桶退化为全局均值并给更宽区间；
3. 预测锚点为最晚观测之后的下一个 UTC 零点（锚点只来自数据，保证可复算）；
4. 每个预测点给出 `point / lower / upper`（正态分位数，置信水平可配）；
5. **服务区峰值** = 预测窗内需求曲线最高点及其置信区间、保守上下界；
6. **部署区域峰值**按区域映射权重聚合各服务区曲线。

## 快速上手（离线命令）

```bash
# 1. 导入输入 / 参数 / 区域映射（重复导入同内容返回同一版本）
python -m service_09251_006.cli import-raw --file flow.json --actor zhang
python -m service_09251_006.cli import-raw --file demand.json
python -m service_09251_006.cli import-params --file params.json
python -m service_09251_006.cli import-mapping --file mapping.json

# 2. 建立情景并派生提交链
python -m service_09251_006.cli scenario-create --params v000003 --mapping v000004
python -m service_09251_006.cli derive <scn_id> add-input v000001
python -m service_09251_006.cli derive <scn_id> add-input v000002
# 并发修订时可声明父提交做乐观并发控制：--base <commit_id>

# 3. 计算（提交为长作业，可异步 worker 排空，也可 --wait / run-job 同步执行）
python -m service_09251_006.cli forecast <scn_id> --wait

# 4. 比较两版预测的服务区峰值与置信区间
python -m service_09251_006.cli compare <fv1> <fv2>

# 5. 采用（关联调度决策）与撤销
python -m service_09251_006.cli adopt <fv> --decision deploy.json --actor li
python -m service_09251_006.cli revoke <fv> --reason "交通团队修订"

# 6. 原始数据更正：历史不变，自动标出受影响下游预测；替换重算后标记核销
python -m service_09251_006.cli correct <old_input_vid> --file corrected.json
python -m service_09251_006.cli list-invalidations

# 7. 谱系核验（进程重启后可用 worker 续跑所有未完成作业）
python -m service_09251_006.cli verify <fv>
python -m service_09251_006.cli worker
```

## HTTP API

```bash
python -m service_09251_006.cli serve --host 0.0.0.0 --port 8080
```

主要端点（请求/响应均为 JSON）：

| 方法 & 路径 | 作用 |
|---|---|
| `POST /api/inputs/raw` · `/api/parameters` · `/api/region-maps` | 导入（幂等） |
| `POST /api/versions/{vid}/corrections` | 登记更正，返回受影响预测 |
| `POST /api/scenarios` / `GET /api/scenarios/{sid}` | 创建情景 / 查看提交链与快照 |
| `POST /api/scenarios/{sid}/derive` | 派生（`add_input`/`replace_input`/`set_parameters`/`set_mapping`，可带 `base_commit_id`） |
| `POST /api/scenarios/{sid}/forecasts` | 提交预测作业（`{"wait": true}` 同步） |
| `GET  /api/jobs/{jid}` / `POST /api/jobs/{jid}/run` / `POST /api/jobs/run-available` | 作业状态、执行/续跑、排空队列 |
| `GET  /api/forecasts/{fvid}` | 预测内容（峰值与置信区间） |
| `GET  /api/forecasts/compare?left=&right=` | 峰值/置信区间对比 |
| `GET  /api/forecasts/{fvid}/verify` | 谱系核验 |
| `POST /api/forecasts/{fvid}/adopt` / `revoke` / `GET …/adoption` | 签署（可附 `decision`）、撤销、事件史 |
| `GET  /api/corrections` · `/api/invalidations` | 更正影响与失效标记 |

错误码：`404 not_found`、`409 conflict/invalid_state/lineage_violation`、
`422 validation_error`，响应体形如 `{"error": {"code": …, "message": …, "details": …}}`。

## 长任务与重启续跑

* 预测作业以 **服务区为检查点粒度**：每完成一个服务区即把中间结果写入 `jobs.cursor` 并续租；
* worker 崩溃后作业保持 `running`，租约到期（`SERVICE_09251_LEASE_SECONDS`，默认 30 秒）
  后可被任意进程认领，已完成服务区在续跑时跳过；
* 预测锚定情景的**具体提交**（不可变），情景继续前进不影响在途作业；
* 同内容预测全局唯一，终态化由指纹唯一约束兜底，多 worker 并发完成只产生一个版本。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

覆盖：重复批次幂等、内容寻址与防篡改、并发派生（同一父提交仅一个成功）、
跨日聚合与峰值置信区间、版本比较、更正后的部分数据失效与重算核销、
签署撤销状态机、长任务崩溃检查点续跑与重启恢复、谱系核验（哈希/指针/精确复算/引擎版本）
以及 HTTP API 全流程。

```bash
python3 -m compileall -q service_09251_006 tests
```
