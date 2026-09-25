# 充电需求预测版本管理

面向业务人员的纯服务端系统：以**不可变版本**保存充电需求预测的输入数据、参数、
区域映射与计算结果，支持从任一版本派生情景、比较服务区峰值与置信区间，并把
被采用的预测与调度决策关联；原始数据更正时不篡改历史，只标记受影响的下游版本。

## 架构

代码按领域模型、应用服务、持久化与接口边界组织；时间、标识通过可替换端口注入，
以便稳定复现状态变化。运行数据与本地配置不写入源码目录。

```
service_09251_006/
├── ports.py      可替换端口：时钟、标识生成器（测试注入固定实现）
├── domain.py     领域模型：版本状态机（DRAFT→COMPUTED→SIGNED→REVOKED）、领域错误
├── forecast.py   纯计算：跨日聚合、逐小时均值/标准差/置信区间、峰值标记、结果哈希
├── storage.py    SQLite 仓储：只增不改的表、事务、线程本地连接（WAL）
├── jobs.py       长任务运行器：按服务区分步检查点，中断/重启后从断点续跑
├── services.py   应用服务门面：导入、派生、计算、签署、撤销、更正影响、比较、谱系核验
├── api.py        HTTP API 边界（标准库实现，无外部依赖）
└── manage.py     离线管理命令（argparse）
```

### 核心规则

- **不可变**：批次、参数集、区域映射、计算结果写入后永不修改；版本状态迁移只追加
  标记（签署、撤销、stale），历史完整可查。
- **幂等导入**：批次/参数/映射按内容哈希去重，重复导入返回既有记录。
- **派生**：`derive` 从任一版本（含已撤销）派生情景，未指定的输入沿父版本继承；
  兄弟序号在事务内分配，并发派生安全。
- **更正影响**：更正批次（`corrects`）登记后，凡输入批次与受影响站点（可按站点、
  时间范围限定）有交集的版本即被标记 `stale`；此后创建时沿用被更正批次的版本
  即建即标。stale 版本禁止被调度决策采用。
- **决策关联**：调度决策只能指向 SIGNED 且未 stale 的版本，部署依据可完整回溯。
- **谱系核验**：沿父链校验输入存在性、内容哈希（参数/映射/批次/结果）与
  stale 标记一致性。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

覆盖：重复批次、并发派生、跨日聚合（含跨午夜归桶）、部分数据失效、
长任务中断续跑与重启续跑、签署/撤销生命周期、HTTP API 与离线命令全链路。

## 编译检查

```bash
python3 -m compileall -q service_09251_006 tests
```

## 离线管理命令

数据库路径：`--db` 选项 > 环境变量 `S09251_006_DB` >
`~/.local/share/service_09251_006/app.db`。

```bash
M="python3 -m service_09251_006.manage"

$M import-batch batch.json                       # 幂等导入原始批次
$M import-batch fix.json --corrects <批次> --correction-stations S1,S2 \
    --correction-reason 仪表校准                  # 更正导入并标记下游
$M create-params '{"growth_factor": 1.2, "confidence": 0.9}'
$M create-mapping '{"S1": "服务区甲"}'
$M create-version --batches <批次> --params <参数> --mapping <映射> --scenario 国庆基准
$M derive <版本> --scenario 高情景 [--params <参数>]
$M compute <版本>          # 可重入；中断后再次执行即续跑
$M resume-job <任务>       # 或按任务 ID 续跑
$M sign <版本> --by 张三
$M decide <版本> --area 服务区甲 --action 部署移动充电车 --units 2 --by 李四
$M compare <版本A> <版本B>  # 服务区峰值与置信区间对比
$M verify-lineage <版本>    # 谱系核验
$M revoke <版本> --by 王五 --reason 假设更新
$M serve --port 8080        # 启动 HTTP API
```

## HTTP API

`manage serve` 启动；所有响应为 JSON，错误形如
`{"error": {"code", "message", "details"}}`。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/batches` | 导入批次（可带 `corrects`/`correction_stations`/`correction_range`） |
| POST | `/api/corrections` | 为已导入批次登记更正 |
| POST | `/api/parameter-sets` · `/api/region-mappings` | 创建参数集 / 区域映射 |
| POST | `/api/versions` | 创建根版本 |
| POST | `/api/versions/{id}/derive` | 派生情景 |
| POST | `/api/versions/{id}/compute` | 发起计算（`{"wait": true}` 同步等待，否则后台） |
| POST | `/api/jobs/{id}/resume` | 续跑中断任务 |
| POST | `/api/versions/{id}/sign` · `/revoke` | 签署 / 撤销 |
| GET | `/api/versions/{id}/results` | 计算结果 |
| GET | `/api/versions/{id}/lineage` | 谱系核验 |
| GET | `/api/compare?a=..&b=..` | 峰值与置信区间比较 |
| POST | `/api/decisions` | 关联调度决策（仅 SIGNED 且未 stale） |
| GET | `/api/versions/{id}/decisions` | 版本关联的决策 |

## 输入数据格式

批次文件为 JSON：`source` 为来源，`records` 为逐站逐小时观测：

```json
{
  "source": "highway-bureau",
  "records": [
    {"station_id": "S1", "observed_at": "2026-09-20T08:00:00Z",
     "vehicles": 120, "sessions": 30, "energy_kwh": 540.0}
  ]
}
```

预测模型 `hourly-mean`：先把记录按（服务区， 自然日， 小时）归桶求和（跨午夜记录
各归其时间戳所在的日与小时），再跨日统计均值与样本标准差，乘以增长/节假日系数，
置信区间为 `mean ± z·std/√n`；峰值取均值最大的小时（并列取最早）。
