# generic_test 通用测试场景说明

这个目录用于做“通用图模型压测场景”，特点是：

- 不依赖固定业务数据集
- 由用户提供 schema
- 根据传入参数动态生成静态/动态 CSV
- 执行导入时优先走 `COPY`
- 如果 `COPY` 失败，则自动回退到 `UNWIND + MERGE`

目录中当前包含：

- `SAM_schema_test.cypher`
  - 示例 schema
- `SAM_copy_test.cypher`
  - 示例 COPY 映射
- `generate_csv_generic.py`
  - 通用 CSV 数据生成脚本
- `perf_write_SF_qtest.py`
  - 通用导入性能测试脚本，支持 `COPY + UNWIND fallback`

如果要完成一轮完整测试，通常顺序是：

1. 准备 schema
2. 使用 `generate_csv_generic.py` 生成 CSV
3. 使用 `perf_write_SF_qtest.py` 导入数据并统计性能
4. 查看结果文件中的批次与汇总信息

## 场景说明

`generic_test` 更像一个“基于 schema 的通用压测框架”，而不是绑定某个固定业务模型的数据集。

脚本会从 schema 中自动识别：

- 有哪些节点表
- 有哪些关系表
- 每条关系的起点表、终点表和基数类型
- 表中有哪些属性列

然后根据参数生成：

- 静态节点数据
- 动态节点数据
- 动态关系数据

这类场景适合用来验证：

- 新 schema 的建模是否能跑通 CSV 导入
- 单节点表大批量静态数据导入性能
- 动态节点 + 动态关系混合数据导入性能
- `COPY` 失败场景下，`UNWIND + MERGE` 兜底链路是否正确

## 当前示例 schema

当前目录中的示例 `SAM_schema_test.cypher` 定义了：

- 节点表
  - `User`
  - `Session`
  - `NASDevice`
  - `Domain`
- 关系表
  - `User_owns_Session`
  - `Session_via_NAS`
  - `User_belongsTo_Domain`

这是一个很适合做网络会话/账号归属类测试的最小通用样例。

## 目录结构

生成数据后，输出目录通常长这样：

```text
generic_test/
├── SAM_schema_test.cypher
├── SAM_copy_test.cypher
├── generate_csv_generic.py
├── perf_write_SF_qtest.py
└── CsvBasic/
    ├── static/
    └── dynamic/
```

- `static/`
  - 保存静态节点表 CSV，例如 `User_0_0.csv`
- `dynamic/`
  - 保存动态节点表和动态关系表 CSV，例如 `Session_0_0.csv`、`User_owns_Session_0_0.csv`

## 一、如何生成数据

### 1. 生成逻辑

`generate_csv_generic.py` 的核心思路是：

1. 读取 schema
2. 解析节点表和关系表定义
3. 根据参数决定哪些节点表作为 static 表
4. 给 static 表按固定行数生成数据
5. 将剩余节点表作为 dynamic 节点表
6. 根据关系拓扑和关系基数，自动估算 dynamic 节点表与关系表的权重
7. 按天分配 dynamic 数据量
8. 按 batch size 输出成多个 shard 文件

它不依赖外部参考数据，也不依赖固定计数表，完全由 schema 和命令行参数驱动。

### 2. 静态数据与动态数据的区别

- static 数据
  - 只生成节点表
  - 由 `--static-tables` 指定哪些表属于 static
  - 每个 static 表都生成 `--static-table-rows` 行
- dynamic 数据
  - 会生成剩余节点表和所有关系表
  - 总量由 `--dynamic-node-total` 和 `--dynamic-rel-total` 控制
  - 每天最多生成 `--daily-total` 行 dynamic 数据

### 3. 你给出的静态数据示例

你给出的这个命令，含义是“通用测试场景，只生成 1000 万行静态 `Session` 数据，不生成 dynamic 数据”：

```bash
python3 generic_test/generate_csv_generic.py \
  --schema generic_test/SAM_schema_test.cypher \
  --out generic_test/CsvBasic \
  --dynamic-node-total 0 \
  --dynamic-rel-total 0 \
  --daily-total 10000000 \
  --dynamic-batch-size 100000 \
  --static-tables Session \
  --static-table-rows 10000000
```

这条命令的效果是：

- `Session` 作为 static 表
- 生成 `10,000,000` 行 `Session`
- 不生成任何 dynamic 节点
- 不生成任何 dynamic 关系
- static 数据会切分成多个文件，每个文件最多 `100,000` 行

### 4. 常见生成场景

#### 场景 A：只生成静态数据

例如只测一个超大静态节点表：

```bash
python3 generic_test/generate_csv_generic.py \
  --schema generic_test/SAM_schema_test.cypher \
  --out generic_test/CsvBasic \
  --dynamic-node-total 0 \
  --dynamic-rel-total 0 \
  --daily-total 10000000 \
  --dynamic-batch-size 100000 \
  --static-tables Session \
  --static-table-rows 10000000
```

#### 场景 B：生成静态数据 + 动态数据

例如让 `User`、`NASDevice`、`Domain` 做静态维表，`Session` 和关系做动态数据：

```bash
python3 generic_test/generate_csv_generic.py \
  --schema generic_test/SAM_schema_test.cypher \
  --out generic_test/CsvBasic \
  --dynamic-node-total 5000000 \
  --dynamic-rel-total 15000000 \
  --daily-total 10000000 \
  --dynamic-batch-size 100000 \
  --static-tables User,NASDevice,Domain \
  --static-table-rows 1000000
```

这样脚本会：

- 先生成 `User`、`NASDevice`、`Domain` 三张静态表
- 再把 `Session` 当作动态节点表生成
- 按关系定义自动生成三张关系表的数据

### 5. 常用参数说明

- `--schema`
  - schema 文件路径，必填
- `--out`
  - 输出目录，默认 `CsvBasic`
- `--dynamic-node-total`
  - 要生成的 dynamic 节点总行数
- `--dynamic-rel-total`
  - 要生成的 dynamic 关系总行数
- `--daily-total`
  - 每天最多生成多少行 dynamic 数据
- `--dynamic-batch-size`
  - 每个 dynamic CSV 文件的最大行数
- `--static-tables`
  - 逗号分隔的静态节点表名列表，例如 `User,Domain`
- `--static-table-rows`
  - 每个 static 表生成多少行
- `--static-batch-size`
  - 每个 static CSV 文件的最大行数，默认 `100000`
- `--seed`
  - 随机种子，默认 `42`

### 6. 文件命名规则

- 静态节点文件
  - `表名_0_分片号.csv`
- 动态节点文件
  - `表名_天号_分片号.csv`
- 动态关系文件
  - `关系表名_天号_分片号.csv`

例如：

- `static/Session_0_0.csv`
- `dynamic/Session_0_0.csv`
- `dynamic/User_owns_Session_3_5.csv`

### 7. 生成完成后怎么看

生成脚本结束时会打印：

```text
Done. dynamic_node_rows=... dynamic_rel_rows=...
Output static: ...
Output dynamic: ...
```

可以用下面的命令快速确认文件是否生成：

```bash
find generic_test/CsvBasic -maxdepth 2 -type f | head
```

## 二、如何执行测试

### 1. 测试脚本行为

`perf_write_SF_qtest.py` 的行为是：

1. 打开目标数据库
2. 执行 schema 中的建表语句
3. 解析 copy 文件
4. 自动展开 shard CSV 文件
5. 优先执行 `COPY`
6. 如果某个文件 `COPY` 失败，则自动读取该 CSV
7. 回退为 `UNWIND + MERGE` 逐批写入
8. 每一组导入完成后关闭并重新打开数据库
9. 最终输出节点、关系、fallback、reopen 的统计结果

所以这个脚本不只是“验证 copy”，也会验证“copy 失败后的 fallback 能否完成导入”。

### 2. 你给出的测试命令

你给出的命令，在当前仓库结构下可以写成：

```bash
python3 generic_test/perf_write_SF_qtest.py \
  --schema generic_test/SAM_schema_test.cypher \
  --copy generic_test/SAM_copy_test.cypher \
  --csv-root generic_test/CsvBasic \
  --db-path /home/eric/install/lbug_main/data/test_db.lbug \
  --result-file generic_test/perf_benchmark_result
```

如果你在其他目录部署脚本和数据，也可以像你提供的绝对路径方式那样执行：

```bash
python3 perf_write_SF_qtest.py \
  --schema /home/eric/project/open_ladybug/LDBC/SAM_schema_test.cypher \
  --copy /home/eric/project/open_ladybug/LDBC/SAM_copy_test.cypher \
  --csv-root /home/eric/project/open_ladybug/LDBC/CsvBasic \
  --db-path /home/eric/install/lbug_main/data/test_db.lbug \
  --result-file /home/eric/project/open_ladybug/LDBC/perf_benchmark_result
```

### 3. 常用参数说明

- `--schema`
  - schema 文件路径
- `--copy`
  - copy 文件路径
- `--csv-root`
  - CSV 根目录，脚本会在其下寻找 `static/` 和 `dynamic/`
- `--db-path`
  - Ladybug 数据库文件路径
- `--result-file`
  - 结果输出文件
- `--buffer-pool-size`
  - 可选，指定 Ladybug buffer pool 大小
- `--max-db-size`
  - 可选，指定数据库最大大小
- `--max-num-threads`
  - 可选，指定执行线程数
- `--unwind-batch-size`
  - `COPY` 失败后，每条 `UNWIND + MERGE` 语句写多少行，默认 `10000`
- `--static-reopen-rows`
  - static 数据累计达到多少行后，完成一组并 reopen，默认 `10000000`

### 4. COPY 与 UNWIND fallback 说明

这是这个通用测试脚本最关键的能力之一。

当某个 `COPY` 语句执行失败时，脚本不会直接终止，而是会：

1. 记录一条 `[copy-failed]`
2. 读取失败的 CSV 文件
3. 构造 `UNWIND [...] AS row`
4. 对节点执行 `MERGE`
5. 对关系执行 `MATCH src/dst + MERGE rel`
6. 记录 fallback 每个 chunk 的耗时和吞吐

因此，这个脚本很适合用来验证：

- `COPY` 正常时的性能
- `COPY` 异常时系统是否仍能导完数据
- `UNWIND + MERGE` 的兜底链路性能

## 三、结果文件怎么看

`--result-file` 会记录多种日志：

- `[head]`
  - 本次测试的参数信息
- `[group]`
  - 当前导入组的信息
- `[copy]`
  - 某个文件 `COPY` 成功时的耗时和吞吐
- `[copy-failed]`
  - 某个文件 `COPY` 失败
- `[fallback-sql]`
  - fallback 生成的 UNWIND SQL
- `[fallback-copy]`
  - fallback 每个 chunk 的写入耗时和吞吐
- `[reopen]` / `[close]`
  - 每组导入之后关闭并重开数据库的耗时
- `[summary]`
  - 节点/关系总耗时、reopen 累计耗时、跳过文件数、fallback 文件数、fallback 行数

如果你只想看最后汇总，可以执行：

```bash
tail -n 20 generic_test/perf_benchmark_result
```

## 四、推荐执行流程

### 场景 A：只测静态 1000 万 Session

```bash
python3 generic_test/generate_csv_generic.py \
  --schema generic_test/SAM_schema_test.cypher \
  --out generic_test/CsvBasic \
  --dynamic-node-total 0 \
  --dynamic-rel-total 0 \
  --daily-total 10000000 \
  --dynamic-batch-size 100000 \
  --static-tables Session \
  --static-table-rows 10000000

python3 generic_test/perf_write_SF_qtest.py \
  --schema generic_test/SAM_schema_test.cypher \
  --copy generic_test/SAM_copy_test.cypher \
  --csv-root generic_test/CsvBasic \
  --db-path /home/eric/install/lbug_main/data/test_db.lbug \
  --result-file generic_test/perf_benchmark_result
```

这个场景适合验证：

- 大静态节点表的导入吞吐
- `COPY Session` 的执行性能

### 场景 B：测静态维表 + 动态 Session + 动态关系

```bash
python3 generic_test/generate_csv_generic.py \
  --schema generic_test/SAM_schema_test.cypher \
  --out generic_test/CsvBasic \
  --dynamic-node-total 5000000 \
  --dynamic-rel-total 15000000 \
  --daily-total 10000000 \
  --dynamic-batch-size 100000 \
  --static-tables User,NASDevice,Domain \
  --static-table-rows 1000000

python3 generic_test/perf_write_SF_qtest.py \
  --schema generic_test/SAM_schema_test.cypher \
  --copy generic_test/SAM_copy_test.cypher \
  --csv-root generic_test/CsvBasic \
  --db-path /home/eric/install/lbug_main/data/test_db.lbug \
  --result-file generic_test/perf_benchmark_result
```

这个场景适合验证：

- static + dynamic 混合导入
- 按天分组的 dynamic 导入
- 节点和关系联合导入性能

## 五、注意事项

- `SAM_copy_test.cypher` 当前默认映射的是：
  - `static/User_0_0.csv`
  - `static/Session_0_0.csv`
  - `static/NASDevice_0_0.csv`
  - `static/Domain_0_0.csv`
  - `dynamic/User_owns_Session_0_0.csv`
  - `dynamic/Session_via_NAS_0_0.csv`
  - `dynamic/User_belongsTo_Domain_0_0.csv`
- 但测试脚本会自动展开同名前缀的 shard 文件
  - 所以即使实际生成了 `*_0_1.csv`、`*_0_2.csv` 等分片，也会被自动识别
- 如果你只生成了某一部分数据，copy 文件中其余表找不到时，脚本会记为 `[skip]`
  - 不会因为文件不存在就立刻失败
- 如果 CSV 只有表头没有数据，脚本会跳过该文件，并记录 `[skip] reason=csv_empty`
- 如果要快速验证链路，建议先用较小规模参数生成少量数据

## 六、一个最小可复现示例

如果想先验证整套流程，可以先跑一个更小的例子：

```bash
python3 generic_test/generate_csv_generic.py \
  --schema generic_test/SAM_schema_test.cypher \
  --out generic_test/CsvBasic \
  --dynamic-node-total 100000 \
  --dynamic-rel-total 300000 \
  --daily-total 100000 \
  --dynamic-batch-size 10000 \
  --static-tables User,NASDevice,Domain \
  --static-table-rows 10000
```

然后执行：

```bash
python3 generic_test/perf_write_SF_qtest.py \
  --schema generic_test/SAM_schema_test.cypher \
  --copy generic_test/SAM_copy_test.cypher \
  --csv-root generic_test/CsvBasic \
  --db-path /home/eric/install/lbug_main/data/test_db.lbug \
  --result-file generic_test/perf_benchmark_result
```

这样可以先确认：

- schema 是否能正确解析
- CSV 是否按预期生成
- COPY 是否能识别所有分片
- fallback 逻辑是否正常
