# SI_SAM 场景测试说明

这个目录用于做 `SI + SAM` 组合场景的图数据生成与写入性能测试，目录中已经包含：

- `SI_SAM_schema.cypher`
  - SI_SAM 场景的建表 schema
- `SI_SAM_copy.cypher`
  - CSV 到表的 `COPY` 映射
- `ladybug_table_counts.csv`
  - 各表参考行数，用于按比例生成数据
- `generate_csv_SI_SAM_mutil_row.py`
  - 生成测试 CSV 的脚本
- `perf_write_SF_SI_SAM.py`
  - 执行导入并统计写入性能的脚本

如果要完成一轮完整测试，通常顺序是：

1. 根据 `SI_SAM_schema.cypher` 和 `ladybug_table_counts.csv` 生成 `CsvBasic` 数据
2. 使用 `perf_write_SF_SI_SAM.py` 自动建库并执行 schema
3. 使用 `perf_write_SF_SI_SAM.py` 执行 `COPY` 导入
4. 查看 `result-file` 中记录的批次耗时和吞吐

## 场景说明

`SI_SAM` 场景描述的是一组校园业务数据与上网认证/会话行为数据的组合测试场景，主要包含三类数据：

- 人员与组织类实体
  - 例如 `User`、`Student`、`Teacher`、`Group`、`Post`、`Label`
- 校园业务事件类实体
  - 例如 `EventAuthLog`、`EventCardConsume`、`EventTeacherComplaint`、`EventTeacherDiscipline`、`TeacherEducation`、`ResearchOutput`
- 上网会话与设备类实体
  - 例如 `EventOnlineSession`、`AccessDevice`、`NASDevice`、`TerminalDevice`、`Gateway`、`IPAddress`、`Service`、`Operator`、`NetworkAccessPoint`

同时，schema 中还定义了多种关系类型，例如：

- 人员与岗位、部门、区域的关系
- 教师与科研成果、教育经历、投诉、处分的关系
- 在线会话与用户、区域、终端、网关、NAS、AP、运营商、服务的关系

这个场景适合验证以下能力：

- 混合节点/边规模下的 CSV 批量导入性能
- 节点优先、关系随后导入时的整体写入吞吐
- 按天分批导入 dynamic 数据时的稳定性与恢复能力

## 目录结构

生成完成后，输出目录通常是下面这种结构：

```text
SI_SAM/
├── SI_SAM_schema.cypher
├── SI_SAM_copy.cypher
├── ladybug_table_counts.csv
├── generate_csv_SI_SAM_mutil_row.py
├── perf_write_SF_SI_SAM.py
└── CsvBasic/
    ├── static/
    └── dynamic/
```

- `static/`
  - 保存静态表数据，文件名通常是 `表名_0_分片号.csv`
- `dynamic/`
  - 保存动态表数据，文件名通常是 `表名_天号_分片号.csv`

## 一、如何生成数据

### 1. 生成逻辑

`generate_csv_SI_SAM_mutil_row.py` 会：

1. 读取 `SI_SAM_schema.cypher`，识别所有节点表和关系表
2. 读取 `ladybug_table_counts.csv`，获取参考表行数
3. 先生成静态数据
4. 再按比例生成动态数据
5. 将每张表按 `--batch-size` 切成多个 shard 文件

其中有两个很重要的规则：

- 固定静态节点表
  - `User`、`Group`、`Teacher`、`Student`、`Post`、`Label`
  - 这些表的行数保持与 `ladybug_table_counts.csv` 一致
- 动态数据按天生成
  - 脚本内部按“每天总计 1000 万行”推进
  - 其中节点约占四分之一，关系约占四分之三

### 2. 生成命令

在仓库根目录下执行：

```bash
python3.11 SI_SAM/generate_csv_SI_SAM_mutil_row.py \
  --schema SI_SAM/SI_SAM_schema.cypher \
  --counts SI_SAM/ladybug_table_counts.csv \
  --out SI_SAM/CsvBasic \
  --batch-size 100000 \
  --node-total 1000000000 \
  --rel-total 3000000000
```

这条命令表示：

- 动态节点总量目标为 `1,000,000,000`
- 动态关系总量目标为 `3,000,000,000`
- 每个输出 CSV 分片最多 `100,000` 行
- 输出到 `SI_SAM/CsvBasic/`

### 3. 常用参数说明

- `--schema`
  - schema 文件路径，通常传 `SI_SAM/SI_SAM_schema.cypher`
- `--counts`
  - 表行数参考文件，通常传 `SI_SAM/ladybug_table_counts.csv`
- `--out`
  - 输出目录，推荐传 `SI_SAM/CsvBasic`
- `--batch-size`
  - 每个 CSV shard 的最大行数
- `--node-total`
  - 目标动态节点总行数
- `--rel-total`
  - 目标动态关系总行数
- `--seed`
  - 随机种子，默认 `42`
- `--memory-limit-gb`
  - 可选的内存上限，默认 `0`，表示自动使用机器约 70% 物理内存预算

### 4. 生成结果说明

脚本完成后会输出类似信息：

```text
Done. workers=...
Output static: SI_SAM/CsvBasic/static
Output dynamic: SI_SAM/CsvBasic/dynamic
```

此时可以检查：

```bash
find SI_SAM/CsvBasic -maxdepth 2 -type f | head
```

如果数据规模很大，生成过程会周期性打印进度信息，例如已完成文件数、剩余文件数和最近生成的文件。

## 二、如何准备数据库

最新的 `perf_write_SF_SI_SAM.py` 已经改成“从空库开始导入”的模式：

- 启动时会删除已存在的 `--db-path`
- 会自动打开新库
- 会执行 `SI_SAM_schema.cypher` 中的 DDL

因此通常不需要再手动提前建库。你只需要确认：

- `--db-path` 指向的是一个允许被重建的数据库文件路径
- 运行脚本的用户对该路径有删除和写入权限

如果 `--db-path` 已经存在且是一个文件，脚本会先删除它再开始导入。

## 三、如何执行测试

### 1. 执行命令

在仓库根目录下执行：

```bash
python3.11 SI_SAM/perf_write_SF_SI_SAM.py \
  --schema SI_SAM/SI_SAM_schema.cypher \
  --copy SI_SAM/SI_SAM_copy.cypher \
  --csv-root SI_SAM/CsvBasic \
  --db-path /root/eric/install/ldbc_sf300.lbug \
  --result-file SI_SAM/perf_benchmark_result
```

如果你的数据和库放在绝对路径下，也可以写成你给出的这种形式：

```bash
python3.11 SI_SAM/perf_write_SF_SI_SAM.py \
  --schema /root/eric/LDBC/SI_SAM_schema.cypher \
  --copy /root/eric/LDBC/SI_SAM_copy.cypher \
  --csv-root /root/eric/LDBC/CsvBasic \
  --db-path /root/eric/install/ldbc_sf300.lbug \
  --result-file /root/eric/LDBC/perf_benchmark_result
```

### 2. 常用参数说明

- `--schema`
  - schema 文件路径
- `--copy`
  - copy 映射文件路径
- `--csv-root`
  - CSV 根目录，脚本会在其下自动扫描 `static/` 和 `dynamic/`
- `--db-path`
  - Ladybug 数据库文件路径
- `--result-file`
  - 性能结果输出文件
- `--assumed-rows`
  - 当使用估算模式时，每个 CSV 文件按多少行计算吞吐，默认 `50000000`
- `--row-count-mode`
  - `actual` 或 `assumed`
  - 默认是 `actual`，会逐个统计 CSV 实际行数
- `--buffer-pool-size`
  - buffer pool 大小，默认 `32 GiB`
- `--max-num-threads`
  - 执行线程数，默认 `32`
- `--start-day`
  - 只导入 `day >= start-day` 的 dynamic 数据，默认 `150`

### 3. 当前脚本行为说明

这个脚本现在已经具备“从零开始建库并执行 schema”的能力，但仍有一个重要限制需要注意：

- 它会删除旧数据库文件
  - 代码里调用了 `remove_existing_db(db_path)`
- 它会执行 schema DDL
  - 代码里会逐条执行 `SI_SAM_schema.cypher`
- 它默认只从 `day >= 150` 的 dynamic 分片开始导入
  - 由 `--start-day` 控制，默认值是 `150`
- 它当前仍然不会真正执行 static CSV 导入
  - `static_jobs` 虽然定义了，但当前逻辑没有把 static 文件加入该列表

也就是说，按当前仓库中的脚本逻辑：

- 适合做“删除旧库 -> 重建 schema -> 从某一天开始导入 dynamic”的测试
- 还不适合直接拿来做“从空库开始导入全部 static + dynamic”的完整首轮测试

如果你只是想重建库并导入 `day >= 150` 的 dynamic 数据，可以直接执行上面的命令。

如果你要尽量跑完整一些，建议至少确认两点：

1. static 数据是否需要通过别的方式提前导入
2. `--start-day` 是否需要改成 `0`

例如，从第 0 天开始导入 dynamic：

```bash
python3.11 SI_SAM/perf_write_SF_SI_SAM.py \
  --schema SI_SAM/SI_SAM_schema.cypher \
  --copy SI_SAM/SI_SAM_copy.cypher \
  --csv-root SI_SAM/CsvBasic \
  --db-path /root/eric/install/ldbc_sf300.lbug \
  --result-file SI_SAM/perf_benchmark_result \
  --start-day 0
```

但即使这样，脚本当前仍然不会自动执行 static CSV 导入。

## 四、测试结果怎么看

`result-file` 中会追加记录如下几类信息：

- `[head]`
  - 本次测试使用的 schema、copy、csv-root、db-path、线程数、起始 day 等参数
- `[copy]`
  - 每个 CSV 分片的导入耗时、行数和 `rows/s`
- `[day-summary]`
  - 某一天所有 dynamic 分片的汇总性能
- `[reopen]` / `[close]`
  - 分组导入完成后关闭并重开数据库的耗时
- `[summary]`
  - 节点、关系、reopen 总耗时以及跳过的 COPY 数量

如果你只关心汇总结果，可以直接看：

```bash
tail -n 20 SI_SAM/perf_benchmark_result
```

## 五、建议执行流程

### 场景 A：生成数据 + 续跑 dynamic 导入

```bash
python3.11 SI_SAM/generate_csv_SI_SAM_mutil_row.py \
  --schema SI_SAM/SI_SAM_schema.cypher \
  --counts SI_SAM/ladybug_table_counts.csv \
  --out SI_SAM/CsvBasic \
  --batch-size 100000 \
  --node-total 1000000000 \
  --rel-total 3000000000

python3.11 SI_SAM/perf_write_SF_SI_SAM.py \
  --schema SI_SAM/SI_SAM_schema.cypher \
  --copy SI_SAM/SI_SAM_copy.cypher \
  --csv-root SI_SAM/CsvBasic \
  --db-path /root/eric/install/ldbc_sf300.lbug \
  --result-file SI_SAM/perf_benchmark_result \
  --start-day 150
```

这个场景下脚本会：

- 删除已有的 `ldbc_sf300.lbug`
- 新建数据库并执行 schema
- 跳过 `day < 150` 的 dynamic 文件
- 导入 `day >= 150` 的 dynamic 文件

### 场景 B：希望从头开始导入 dynamic

```bash
python3.11 SI_SAM/perf_write_SF_SI_SAM.py \
  --schema SI_SAM/SI_SAM_schema.cypher \
  --copy SI_SAM/SI_SAM_copy.cypher \
  --csv-root SI_SAM/CsvBasic \
  --db-path /root/eric/install/ldbc_sf300.lbug \
  --result-file SI_SAM/perf_benchmark_result \
  --start-day 0
```

前提仍然是：

- static 数据如果必需，需要通过其他方式提前导入

在这个场景下，脚本会从第 0 天开始导入 dynamic，但仍不会自动导入 static CSV。

## 六、注意事项

- `perf_write_SF_SI_SAM.py` 中默认 `--copy` 指向的是 `sam_copy.cypher`
  - 但当前目录里的实际文件名是 `SI_SAM_copy.cypher`
  - 因此执行时建议始终显式传入 `--copy SI_SAM/SI_SAM_copy.cypher`
- `perf_write_SF_SI_SAM.py` 会删除 `--db-path` 指向的已有数据库文件
  - 执行前请确认该路径下的库可以被覆盖
- 生成 10 亿节点、30 亿关系会产生非常大的磁盘占用和执行时间
  - 请提前确认磁盘空间、内存和 CPU 资源
- `--row-count-mode actual` 会逐个统计 CSV 实际行数
  - 在文件很多时会增加额外扫描成本
- 当前脚本会执行 schema，但 static CSV 仍未进入实际导入流程
  - 如果测试依赖 static 数据，需要额外处理
- 如果只想快速跑通流程，可以先把 `--node-total`、`--rel-total` 和 `--batch-size` 调小做小规模验证

## 七、一个最小可复现流程

如果你想先验证脚本链路是否正常，可以先用较小参数：

```bash
python3.11 SI_SAM/generate_csv_SI_SAM_mutil_row.py \
  --schema SI_SAM/SI_SAM_schema.cypher \
  --counts SI_SAM/ladybug_table_counts.csv \
  --out SI_SAM/CsvBasic \
  --batch-size 10000 \
  --node-total 100000 \
  --rel-total 300000
```

然后执行导入脚本：

```bash
python3.11 SI_SAM/perf_write_SF_SI_SAM.py \
  --schema SI_SAM/SI_SAM_schema.cypher \
  --copy SI_SAM/SI_SAM_copy.cypher \
  --csv-root SI_SAM/CsvBasic \
  --db-path /root/eric/install/ldbc_sf300.lbug \
  --result-file SI_SAM/perf_benchmark_result \
  --start-day 0
```

这样更适合先确认：

- 数据是否正确生成
- COPY 映射是否能匹配到文件
- 删除旧库、建表、dynamic 导入流程是否能跑通
