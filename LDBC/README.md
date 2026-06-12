# LDBC 场景测试说明

## 数据集下载与解压

这个目录里的 `generate_csv.py` 默认会参考 `LDBC/CsvBasic` 目录中的原始 LDBC CsvBasic 数据集，因此在开始测试前，通常需要先下载并解压一份官方数据。

LDBC 官方数据集下载地址示例：

- `SF0.1`:
  - `https://datasets.ldbcouncil.org/snb-interactive-v1/social_network-sf0.1-CsvBasic-StringDateFormatter.tar.zst`
- `SF1`:
  - `https://datasets.ldbcouncil.org/snb-interactive-v1/social_network-sf1-CsvBasic-StringDateFormatter.tar.zst`
- `SF3`:
  - `https://datasets.ldbcouncil.org/snb-interactive-v1/social_network-sf3-CsvBasic-StringDateFormatter.tar.zst`
- `SF1000`:
  - `https://datasets.ldbcouncil.org/snb-interactive-v1/social_network-sf1000-CsvBasic-StringDateFormatter.tar.zst`

如果你要下载某个 scale factor 的数据，可以直接使用 `wget`。下面是一个通用示例：

```bash
wget --continue \
     --tries=0 \
     --waitretry=30 \
     --retry-connrefused \
     --retry-on-http-error=429,500,502,503,504 \
     --read-timeout=30 \
     --timeout=60 \
     "https://datasets.ldbcouncil.org/snb-interactive-v1/social_network-sf1-CsvBasic-StringDateFormatter.tar.zst"
```

如果你想下载 `SF0.1`，可以把最后的 URL 改成：

```bash
https://datasets.ldbcouncil.org/snb-interactive-v1/social_network-sf0.1-CsvBasic-StringDateFormatter.tar.zst
```

下载完成后，可以使用下面的命令解压：

```bash
tar --use-compress-program=unzstd \
  -xvf social_network-sf0.1-CsvBasic-StringDateFormatter.tar.zst
```

解压后，建议把数据整理到 `LDBC/CsvBasic/` 目录下，供 `generate_csv.py` 直接读取。也就是说，这个目录下通常应该能看到类似下面的结构：

```text
LDBC/CsvBasic/static/
LDBC/CsvBasic/dynamic/
```

这个目录用于做 LDBC 场景测试，主要包含两类内容：

- LDBC 图模型的 schema / COPY 脚本
- 生成测试数据与执行写入性能测试的辅助脚本

如果你要跑一套完整流程，通常顺序是：

1. 用 `generate_csv.py` 生成一套更大规模的 LDBC CSV 数据
2. 用 `perf_write_SF.py` 按 schema 和 COPY 脚本把这些 CSV 导入 Ladybug
3. 记录每个批次以及节点 / 边整体的写入性能

## 目录文件说明

- `ldbc_schema.cypher`
  - 定义 LDBC 场景里的节点表和关系表，例如 `Person`、`Post`、`knows`、`likes_Post` 等。
  - 这个文件相当于导入前的建表脚本。

- `ldbc_copy.cypher`
  - 定义每张表默认从哪个 CSV 文件导入。
  - 文件里写的是标准的 `COPY ... FROM ...` 映射关系，供导数脚本解析和执行。

- `ldbc_schema_m_m.cypher`
  - 这是另一份 schema 定义文件，通常用于保存和主 schema 不同的关系约束版本。
  - 如果你在做不同建模方式或关系基数的对比测试，它会有用。

- `generate_csv.py`
  - 用于按给定的总节点数、总关系数，生成一套放大版、分片后的 LDBC CSV 数据集。

- `perf_write_SF.py`
  - 用于执行 schema 建表和 COPY 导入，并统计 Ladybug 的写入性能。

## `generate_csv.py`

这个文件 `LDBC/generate_csv.py` 的作用，可以先概括成一句话：

它会参考已有的 `CsvBasic` 数据集，推断各个 LDBC 节点表和关系表的大致占比，然后按你指定的“总节点数 / 总关系数”生成一套更大的、分片的 CSV 数据集。

源码入口在 [generate_csv.py](/home/eric/project/GBSPT/LDBC/generate_csv.py:1)。

### 整体思路

它不是从 schema 里精确理解每个字段的业务语义后再严格造数，而是走了一个“实用型扩容器”的思路：

1. 读 `ldbc_schema.cypher`，找出有哪些 node table / rel table
2. 读 `ldbc_copy.cypher`，找出每张表原本对应哪个 CSV 文件
3. 去参考数据目录 `CsvBasic` 里数每个参考文件有多少行
4. 按参考数据里的比例，把你输入的总量分配到各个表
5. 给每张表生成随机但结构匹配的 CSV 内容
6. 按 `shard_rows` 切成多个分片文件
7. 用多进程并发写出

### 执行命令

示例：

```bash
python LDBC/generate_csv.py \
  --total-nodes 100000000 \
  --total-rels 500000000 \
  --source-root LDBC/CsvBasic \
  --output-root LDBC/CsvGenerated \
  --shard-rows 50000000 \
  --workers 16
```

常用参数说明：

- `--total-nodes`：目标节点总数
- `--total-rels`：目标关系总数
- `--source-root`：参考数据集目录，默认是 `LDBC/CsvBasic`
- `--output-root`：输出目录，默认是 `LDBC/CsvGenerated`
- `--shard-rows`：每个分片文件的行数
- `--workers`：并发进程数，`0` 表示自动按 CPU 和内存估算

## `perf_write_SF.py`

这个文件 `LDBC/perf_write_SF.py` 的作用，可以先概括成一句话：

它会先执行 LDBC 的建表 schema，再解析 `ldbc_copy.cypher` 中的 CSV 导入定义，把节点表和关系表按批次导入 Ladybug，并输出每个批次以及整体的写入性能统计。

源码入口在 [perf_write_SF.py](/home/eric/project/GBSPT/LDBC/perf_write_SF.py:1)。

### 整体思路

这个脚本的思路比较直接，核心是“按 LDBC schema 建库，再按 COPY 文件批量导数并统计吞吐”：

1. 读取 `ldbc_schema.cypher`，识别哪些表是节点表、哪些是关系表
2. 读取 `ldbc_copy.cypher`，解析每条 `COPY table FROM file`
3. 如果 COPY 指向的是类似 `*_0_0.csv` 的分片名，就自动展开同目录下所有 shard 文件
4. 先执行所有建表语句
5. 按“节点表优先、关系表其次”的顺序执行 COPY
6. 记录每个批次的耗时、估算行数和 rows/s
7. 最后汇总 node / rel 两类数据的总耗时和吞吐

### 执行命令

如果你要导入 `LDBC` 目录下现成的 `static/`、`dynamic/` 数据，可以这样执行：

```bash
python LDBC/perf_write_SF.py \
  --schema LDBC/ldbc_schema.cypher \
  --copy LDBC/ldbc_copy.cypher \
  --csv-root LDBC \
  --db-path LDBC/ldbc_sf01.lbug \
  --result-file LDBC/perf_benchmark_result
```

如果你要导入 `generate_csv.py` 生成出来的 `CsvGenerated` 数据，可以这样执行：

```bash
python LDBC/perf_write_SF.py \
  --schema LDBC/ldbc_schema.cypher \
  --copy LDBC/ldbc_copy.cypher \
  --csv-root LDBC/CsvGenerated \
  --db-path LDBC/ldbc_generated.lbug \
  --result-file LDBC/perf_generated_result \
  --assumed-rows 50000000
```

常用参数说明：

- `--csv-root`：CSV 根目录，脚本会在其下解析 `static/` 和 `dynamic/`
- `--db-path`：输出的 Ladybug 数据库文件
- `--result-file`：性能结果输出文件
- `--assumed-rows`：每个 COPY 文件按多少行估算吞吐
- `--buffer-pool-size`：Ladybug buffer pool 大小，默认是 `48 GiB`

## 建议的测试流程

```bash
python LDBC/generate_csv.py \
  --total-nodes 100000000 \
  --total-rels 500000000 \
  --source-root LDBC/CsvBasic \
  --output-root LDBC/CsvGenerated \
  --shard-rows 50000000

python LDBC/perf_write_SF.py \
  --schema LDBC/ldbc_schema.cypher \
  --copy LDBC/ldbc_copy.cypher \
  --csv-root LDBC/CsvGenerated \
  --db-path LDBC/ldbc_generated.lbug \
  --result-file LDBC/perf_generated_result \
  --assumed-rows 50000000
```

这样就能完成一轮“生成数据 -> 导入数据库 -> 统计写入性能”的 LDBC 场景测试。
