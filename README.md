# csvgate · CSV 严格校验与事务导入回滚

上游每天给一份站点订单 CSV，要求「**全对才入库，错了要知道第几行错哪列**」。
本工具把这份人工核对变成 6 个可复核的后端模块，并为「传错文件」补上可恢复的批次撤回；任一行非法则整份回滚，库内明细行数不变。

使用者：数据平台组入库值班同学。

## 能力

| 模块 | 落地位置 | 做什么 |
|---|---|---|
| 1 上传与编码探测 | `app/encoding.py` | BOM → UTF-8-SIG；严格 UTF-8 失败 → GB18030；都失败 → 400 + 建议。响应记录是否 BOM、前 16 字节摘要、逐次尝试 |
| 2 表头契约 | `app/contract.py` | 必需列缺失=错误；额外列 `reject`/`warn`；列顺序无关；表头去首尾空白；重复列名=错误 |
| 3 行级校验 | `app/rows.py` | 主键严格递增（重复 `duplicate` / 倒序 `out_of_order`，逐行与上一行比较）、int/date/decimal/enum、必填、长度上限、日期真实性 |
| 4 分组聚合 | `app/aggregate.py` | 按 `site` 求和，并用三条独立口径自证一致 |
| 5 事务导入 | `app/importer.py` | 单事务写 批次+明细+聚合；异常即 ROLLBACK；sha256 重复 → `duplicate_file`，可 `force_new_batch` |
| 6 错误报告 | `app/report.py` | 按错误类型汇总 + 逐行依据（行号/列名/原值/原因）+ 修正建议 |
| 7 批次撤回 | `app/retraction.py` | 撤回已入账批次：独有订单移出明细，后来版本保留并在冲突清单逐项标出；明细与汇总一次性切换；进度可恢复、重试幂等 |

编排在 `app/pipeline.py`，路由 `app/main.py` 只做参数解析与渲染。

## 核心规则

1. **编码探测**（证据可查）
   - 前 3 字节是 `EF BB BF` → 按 `utf-8-sig` 解码，BOM 剥离；
   - 否则严格 UTF-8；失败再试 `gb18030`（GBK 超集）；
   - 都失败 → HTTP 400，附三次尝试的失败字节偏移与修复建议。
   - 判定是**确定性**的：纯 ASCII 文件一律算 `utf-8`。
2. **行级错误只报第一处**：同一行同一列按「必填 → 长度 → 类型/枚举 → 业务」取第一个失败原因，
   避免一个超长 site 值同时被报成长度错和枚举错、干扰计数。
3. **主键严格递增**：与「上一条主键可解析的行」比较；相等 → `duplicate`，更小 → `out_of_order`，更大 → 通过并更新基线。
4. **日期真实性**：先匹配 `YYYY-MM-DD`，再用 `date.fromisoformat` 判真实存在（`2026-02-30` → `date_invalid`）。
5. **聚合自证**：分组累加 cents、明细单趟线性累加、`Decimal` 原始字符串复核、组内行数和=明细行数，四条必须同时成立。
6. **事务与重复文件**
   - 全对：`BEGIN IMMEDIATE` 写批次 → 明细 → 聚合 → `COMMIT`；中途异常 `ROLLBACK`，明细行数保持写入前值。
   - 同 `sha256` 已入库且未勾选 `force_new_batch` → 返回 `duplicate_file`，写入 0 行，返回既有批号。
   - 勾选 `force_new_batch` → 新建批次（`-F2`/`-F3` 后缀），旧批次保留且 `superseded_by` 指向新批次，
     语义是「同一份文件被有意重灌，重灌痕迹可追溯」，不是覆盖删除。
7. **批次撤回**（`app/retraction.py`，`retract_batch`）—— 传错文件后的唯一正确出口
   - 撤回一个 `status=active` 的已入账批次：它独有的订单从当前明细里移出，站点汇总同步扣减；
     不需要再建一个批次去「抵消」，也不会让同一订单重复累计。
   - **后来版本保留**：同一 `order_id` 若出现在更晚入库的 active 批次里，被撤回批次自己的那行仍撤下，
     后来批次的行原样保留；这类订单逐项写入**冲突清单**（`action=kept_later_version`，
     附后来批次/行号/修订差异/其它版本），长期可查。
   - **一次性切换**：每个处理块「删明细 + 扣汇总 + 记进度」在同一事务提交，收尾块在同一事务里
     翻转批次状态并核对「站点汇总 == 明细逐项复算」；任何时刻明细与汇总一致。
   - **原文与依据长期保留**：`batches` 行只改为 `status=retracted`（不删）、`batch_aggregates` 原始快照不删、
     `batch_retraction_items` 保存被撤回行逐行快照；撤回原因/操作人/幂等键/前后汇总/时间存
     `batch_retractions`；审计写 `import_attempts(status=retracting/retracted)`。
   - **中断可恢复**：计划与逐块进度先落库；中断后重新发起同一撤回从剩余行继续，已扣减的行不会重复扣减。
   - **重试幂等**：已完成的撤回再次发起只返回既有记录（`idempotent_replay=true`），不写库、不二次扣减。
   - 撤回后相同 sha256 可重新上传（`find_batch_by_sha` 只认 active 批次，不再命中 `duplicate_file`）。
   - 入口：结果页「撤回本批次」、`/review` 批次行「撤回」、`POST /api/batches/{batch_id}/retract`；
     撤回依据页 `/retraction/{batch_id}`、`GET /api/batches/{batch_id}/retraction`。

## 启动

```bash
cd batch-20261114b/projects/gb-623
cp .env.example .env          # 可选，默认端口 18212
docker compose up -d --build
curl -s http://127.0.0.1:18212/health
```

- 页面：<http://127.0.0.1:18212/>
- 数据卷：`csvgate-data:/app/data`（SQLite `csvgate.sqlite3`）
- 启动即幂等 seed：`meta.seed_version = seed-2026-11-07.1`，批次 `B-20261107-001`（60 行历史周批次）；
  重复启动不会重复写入。**重建数据库**：`docker compose down -v && docker compose up -d --build`。

## 演示步骤

1. `/workbench` 下载 `normal_500.csv` 并上传 → 结论「已入库」，得批号、站点聚合表、明细前 20 行。
2. 上传 `broken.csv` → HTTP 422，结论「整体回滚」，列出 5 条逐行错误（重复主键 / 倒序 / 非法日期 / 超长 / 枚举越界）。
   刷新首页或 `/health` 看 `fact_rows` 未变。
3. 上传 `gbk_120.csv` → 编码探测显示 `gb18030`，120 行入库。
4. 再上传一次 `normal_500.csv` → `duplicate_file`，写入 0 行；到 `/review` 点「验证重复导入」看只读判定。
5. 勾选 `force_new_batch` 再传 `normal_500.csv` → 新批次，旧批次标 superseded。
6. 撤回：到 `/review` 点任一 active 批次的「撤回」，填写原因/操作人 → 明细与站点汇总立刻扣减；
   再点一次「撤回」是幂等重试（不重复扣减）。结果页与 `/retraction/{批号}` 可看撤回依据、逐行快照与冲突清单。

命令行等价（验收脚本用的就是这些）：

```bash
curl -s http://127.0.0.1:18212/health
curl -s -F "file=@sample_data/normal_500.csv" -F "extra_policy=reject" http://127.0.0.1:18212/api/import
curl -s -X POST -H 'Content-Type: application/json' \
  -d '{"reason":"值班传错文件","operator":"ops"}' \
  http://127.0.0.1:18212/api/batches/B-20261114-XXXX/retract
curl -s http://127.0.0.1:18212/api/batches/B-20261114-XXXX/retraction
curl -s -o /tmp/broken.json -w '%{http_code}\n' -F "file=@sample_data/broken.csv" http://127.0.0.1:18212/api/import
python3 -c "import json;d=json.load(open('/tmp/broken.json'));print(d['report']['total'],[e['code'] for e in d['report']['rows']])"
```

## API / 页面

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查 + 当前明细行数 + seed 版本 |
| POST | `/api/import` | multipart：`file`、`extra_policy`、`force_new_batch`；200 已入库/重复文件、400 编码失败、422 校验拒绝 |
| POST | `/workbench` | 页面表单同管道；通过 → 303 跳结果页，拒绝 → 422 直接渲染错误表 |
| POST | `/api/contract-check` | 只检查表头（含 `warn` 策略演练） |
| POST | `/api/replay-check` | 按 sha256 判定「再次上传会不会重复」 |
| GET | `/api/batches` | 批次列表 + 统计（含 active / retracted） |
| POST | `/api/batches/{batch_id}/retract` | 撤回已入账批次；JSON `reason`/`operator`/`idempotency_key` 可选；200 完成、409 未完成/不可撤回、404 未知批次 |
| GET | `/api/batches/{batch_id}/retraction` | 撤回依据 + 冲突清单（长期保留） |
| GET | `/api/samples/{name}` | 样例下载 |
| 页面 | `/`、`/workbench`、`/result/{batch_id}`、`/result/attempt/{id}`、`/review`、`/retraction/{batch_id}` | 首页/工作台/结果/错误明细/历史复核/撤回依据 |

## 目录

```text
app/            main.py settings.py pipeline.py seed.py
                encoding.py contract.py rows.py aggregate.py importer.py report.py store.py retraction.py
                templates/ static/
sample_data/    normal_500.csv gbk_120.csv seed_20261107.csv broken.csv contract.json make_samples.py
tests/          test_encoding.py test_rows.py test_importer.py test_api.py test_retraction.py conftest.py
Dockerfile docker-compose.yml requirements.txt .env.example
```

## 已知限制

- CSV 解析用标准库 `csv.reader`：支持引号包裹字段，但**行号**按「数据行 + 1」换算，
  字段内含换行时行号会偏移（当前样例与常见导出不含换行字段）。
- 编码只覆盖 UTF-8 / UTF-8-BOM / GB18030；UTF-16/UTF-32 会走到 400 并给出提示，不做自动识别。
- 分隔符固定逗号，不做 `;`/`\t` 自动猜测；SJIS、Big5 等会按 GB18030 误判成功的概率存在（GBK 超集覆盖广），
  需要在契约里约定来源编码。
- 单次上传上限 8MB（约 10 万行量级）；不做流式分片。
- 明细与批次不提供物理删除接口：撤回只把批次改为 `retracted` 并移出当前明细/汇总，
  原批次、聚合快照、逐行快照与撤回依据长期保留；重灌仍可用 `force_new_batch` + `superseded_by` 追溯。
  撤回是整批操作，不支持按行部分撤回；撤回后如需修正数据，请修正文件后重新上传（同 sha256 可再入库）。
