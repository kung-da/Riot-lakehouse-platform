# Riot Lakehouse Platform

Riot Lakehouse Platform là project Data Lakehouse end-to-end cho dữ liệu Riot Games API. Project mô phỏng một pipeline dữ liệu hiện đại: thu thập raw JSON bằng Python crawler, ingest dữ liệu theo cơ chế incremental, xử lý bằng PySpark qua các lớp Bronze, Silver, Gold, Platinum, lưu trữ trên local hoặc AWS S3, quản lý metadata bằng AWS Glue Data Catalog, truy vấn bằng Athena và phục vụ dashboard trên Power BI.

Project được thiết kế theo hướng portfolio/career-ready: code có cấu trúc rõ ràng, có Docker, Airflow DAGs, pytest, data quality report và CI bằng GitHub Actions.

![Data Lakehouse Architecture](docs/images/Pipeline.png)

## Mục Lục

- [Tổng Quan](#tổng-quan)
- [Kiến Trúc](#kiến-trúc)
- [Data Flow](#data-flow)
- [Tech Stack](#tech-stack)
- [Cấu Trúc Thư Mục](#cấu-trúc-thư-mục)
- [Yêu Cầu Hệ Thống](#yêu-cầu-hệ-thống)
- [Cấu Hình Môi Trường](#cấu-hình-môi-trường)
- [Chạy Bằng Docker](#chạy-bằng-docker)
- [Chạy Python Crawler](#chạy-python-crawler)
- [Chạy Pipeline PySpark](#chạy-pipeline-pyspark)
- [Chạy Airflow](#chạy-airflow)
- [AWS S3, Glue Và Athena](#aws-s3-glue-và-athena)
- [Power BI Dashboard](#power-bi-dashboard)
- [Testing](#testing)
- [CI/CD](#cicd)
- [Monitoring Và Data Quality](#monitoring-và-data-quality)
- [Mở Rộng Project](#mở-rộng-project)
- [Chèn Ảnh Vào README](#chèn-ảnh-vào-readme)

## Tổng Quan

Pipeline xử lý các nhóm dữ liệu Riot Games API như:

- Match Data
- Timeline Data
- Summoner Data
- Ranked/League Data
- Spectator Data, Static Data và các domain khác có thể mở rộng thêm

Luồng chính của project:

1. Python crawler gọi Riot Games API và lưu raw JSON vào `raw/` hoặc S3.
2. Bronze job ingest raw JSON theo append-only, thêm metadata, checkpoint và ghi Parquet.
3. Silver job parse JSON, clean dữ liệu, chuẩn hóa schema, xử lý null/duplicate và ghi các bảng domain.
4. Gold job tạo dimensional model, fact tables và analytics marts phục vụ KPI/dashboard.
5. Platinum layer định nghĩa feature tables phục vụ advanced analytics và AI/ML.
6. Data quality job tạo report JSON/Markdown cho Silver và Gold.
7. Airflow orchestration chạy toàn bộ pipeline theo DAG.
8. Athena/Power BI dùng Glue Data Catalog để query và visualize dữ liệu.

## Kiến Trúc

Kiến trúc Lakehouse được chia thành các vùng trách nhiệm rõ ràng:

| Thành phần | Vai trò |
| --- | --- |
| Riot Games API | Nguồn dữ liệu gameplay, match, ranked, summoner, timeline |
| Python Crawler | Thu thập dữ liệu, kiểm soát incremental, lưu raw JSON |
| Bronze Layer | Lưu raw payload append-only kèm ingestion metadata |
| Silver Layer | Clean, validate, normalize schema và deduplicate |
| Gold Layer | Tạo dimension, fact và data marts cho dashboard |
| Platinum Layer | Feature layer cho AI/ML và advanced analytics |
| AWS S3 | Data lake storage cho raw, lakehouse, checkpoint và report |
| AWS Glue Data Catalog | Catalog metadata cho Athena/BI |
| AWS Athena | Query engine serverless trên S3 |
| Apache Airflow | Orchestration cho từng stage pipeline |
| pytest | Unit tests, data tests và pipeline tests |
| GitHub Actions | Lint, test và nền tảng CI/CD |

## Data Flow

### Raw Input

Crawler hoặc data collection job cần ghi JSON theo layout sau:

```text
raw/
  matches/*.json
  timelines/*.json
  summoners/*.json
  ranked/*.json
```

Khi chạy môi trường production, các path này có thể trỏ tới S3 thông qua `configs/prod.yaml`.

### Bronze Layer

Bronze lưu dữ liệu raw theo append-only:

- Input: `raw/{dataset}/*.json`
- Output local: `data/lakehouse/bronze/raw_json/`
- Output S3: `s3://<bucket>/<lakehouse-prefix>/bronze/raw_json/`
- Format: Parquet, Snappy compression
- Partition: `dataset`, `ingest_date`
- Metadata: `source_file`, `file_hash`, `ingest_ts`, `ingest_date`, `dataset`, `payload`
- Checkpoint: `metadata/checkpoints/{dataset}.json`

Bronze không biến đổi business payload quá sớm. Mục tiêu là giữ lại dữ liệu gốc, có lineage và có thể replay.

### Silver Layer

Silver parse payload Riot API và chuẩn hóa thành các bảng domain:

- `matches`
- `participants`
- `teams`
- `summoners`
- `ranked`
- `timeline_frames`
- `timeline_events`

Silver thực hiện:

- Schema normalization
- Type casting
- Null handling
- Business-key deduplication
- Lineage từ Bronze
- Partition theo `dataset`, `game_date`

### Gold Layer

Gold tạo mô hình analytics phục vụ BI:

Dimensions:

- `dim_date`
- `dim_match`
- `dim_summoner`
- `dim_champion`
- `dim_team`
- `dim_rank`

Facts:

- `fact_participant_performance`
- `fact_team_objectives`
- `fact_rank_snapshot`
- `fact_timeline_frames`
- `fact_timeline_events`

Marts:

- `mart_player_daily_performance`
- `mart_champion_daily_performance`
- `mart_role_daily_performance`
- `mart_rank_daily_summary`
- `mart_team_objective_daily_summary`

### Platinum Layer

Platinum là lớp feature/analytics nâng cao. Repo hiện có registry SQL cho các nhóm feature:

- `match_win_features`
- `player_performance_features`
- `champion_meta_features`

Layer này được thiết kế để mở rộng thành feature tables cho machine learning, ranking model, champion meta analysis hoặc player behavior analytics.

## Tech Stack

| Nhóm | Công nghệ |
| --- | --- |
| Language | Python 3.10+ |
| Processing | PySpark |
| Storage | Local filesystem, AWS S3 |
| File format | Parquet, có thể bật Delta Lake qua config |
| Orchestration | Apache Airflow |
| Catalog | AWS Glue Data Catalog |
| Query | AWS Athena |
| BI | Power BI |
| Testing | pytest |
| Container | Docker, Docker Compose |
| CI | GitHub Actions, ruff, pytest |

## Cấu Trúc Thư Mục

```text
.
├── .github/workflows/        # GitHub Actions CI
├── configs/                  # Config cho dev/prod và table metadata
├── dags/                     # Airflow DAGs
├── data/                     # Local lakehouse output, không commit
├── docs/images/              # Ảnh architecture/dashboard cho README
├── metadata/                 # Checkpoints, Airflow metadata local
├── raw/                      # Raw Riot API JSON, không commit
├── reports/                  # Data quality reports
├── scripts/                  # Airflow entrypoint, helper scripts
├── sql/athena/               # Athena DDL templates
├── src/lakehouse/
│   ├── bronze/               # Bronze ingestion
│   ├── catalog/              # Glue/Athena catalog helpers
│   ├── common/               # Config, Spark, storage, checkpoint
│   ├── gold/                 # Gold transforms and aggregations
│   ├── jobs/                 # CLI entrypoints
│   ├── platinum/             # Feature SQL registry
│   ├── quality/              # Data quality rules and reports
│   ├── raw/                  # Raw file discovery and dataset detection
│   ├── silver/               # Silver cleaners and transformer
│   └── validation/           # Validation helpers
└── tests/                    # Unit, data and pipeline tests
```

Runtime folders như `raw/`, `data/lakehouse/`, `metadata/checkpoints/` và `reports/` thường không nên commit lên GitHub, trừ các file mẫu cần thiết.

## Yêu Cầu Hệ Thống

Cách khuyến nghị là chạy bằng Docker:

- Docker Desktop hoặc Docker Engine
- Docker Compose v2
- Riot Games API key
- AWS account nếu chạy S3/Glue/Athena

Nếu chạy local không dùng Docker:

- Python 3.10+
- Java 17 cho PySpark
- pip
- AWS CLI hoặc AWS credentials nếu dùng S3

## Cấu Hình Môi Trường

Tạo file `.env` từ mẫu:

```powershell
Copy-Item .env.example .env
```

Hoặc trên macOS/Linux:

```bash
cp .env.example .env
```

Các biến quan trọng:

```env
RIOT_API_KEY=

LAKEHOUSE_ENV=dev
LAKEHOUSE_CONFIG_DIR=configs
LAKEHOUSE_ENV_FILE=.env

LAKEHOUSE_RAW_ROOT=raw
LAKEHOUSE_ROOT=data/lakehouse
LAKEHOUSE_CHECKPOINT_ROOT=metadata/checkpoints
LAKEHOUSE_REPORT_ROOT=reports

AWS_REGION=ap-southeast-1
S3_BUCKET=
S3_RAW_PREFIX=raw
S3_LAKEHOUSE_PREFIX=lakehouse
S3_CHECKPOINT_PREFIX=metadata/checkpoints
S3_REPORT_PREFIX=reports
ATHENA_DATABASE=riot_lakehouse

SPARK_MASTER=local[*]
SPARK_DRIVER_MEMORY=4g
SPARK_SHUFFLE_PARTITIONS=8
SPARK_DEFAULT_PARALLELISM=8
SPARK_ENABLE_DELTA=false

AIRFLOW_PORT=8088
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin
```

Config local nằm ở `configs/dev.yaml`. Config production/S3 nằm ở `configs/prod.yaml`.

## Chạy Bằng Docker

Build image:

```bash
docker compose build
```

Chạy Bronze mặc định:

```bash
docker compose run --rm lakehouse
```

Lệnh trên tương đương:

```bash
docker compose run --rm lakehouse python -m lakehouse.jobs.run_bronze --env dev
```

Chạy toàn bộ pipeline Bronze -> Silver -> Gold:

```bash
docker compose run --rm lakehouse python -m lakehouse.jobs.run_full_pipeline --env dev
```

Chạy test trong container:

```bash
docker compose run --rm lakehouse pytest -q
```

## Chạy Python Crawler

Repo lakehouse này xử lý dữ liệu raw JSON đã được crawler ghi ra `raw/` hoặc S3. Crawler nên đảm bảo các nguyên tắc sau:

- Dùng `RIOT_API_KEY` từ `.env`, không hard-code key trong source code.
- Lưu từng API response thành JSON nguyên bản.
- Đặt file theo dataset, ví dụ `raw/matches/VN2_1032611162.json`.
- Duy trì incremental state như `match_id` checkpoint, `last_run_timestamp` hoặc danh sách file đã xử lý.
- Bỏ qua record đã tồn tại để hỗ trợ append-only ingestion.
- Tôn trọng Riot API rate limit.

Ví dụ layout đầu ra sau khi crawler chạy:

```text
raw/
  matches/VN2_1032611162.json
  timelines/VN2_1032611162.json
  summoners/<puuid>.json
  ranked/<queue-or-league>.json
```

Sau khi crawler ghi raw JSON, chạy Bronze để đưa dữ liệu vào lakehouse:

```bash
docker compose run --rm lakehouse python -m lakehouse.jobs.run_bronze --env dev
```

Nếu crawler được đóng gói thành module riêng trong project, nên expose lệnh theo pattern:

```bash
python -m <crawler_module> --platform vn2 --region sea --output raw
```

Sau đó giữ nguyên contract output `raw/{dataset}/*.json` để các job hiện tại hoạt động mà không cần sửa downstream.

## Chạy Pipeline PySpark

### Bronze

Ingest tất cả dataset:

```bash
docker compose run --rm lakehouse python -m lakehouse.jobs.run_bronze --env dev
```

Ingest một số dataset:

```bash
docker compose run --rm lakehouse python -m lakehouse.jobs.run_bronze --env dev --datasets matches timelines
```

Giới hạn số file hoặc batch size:

```bash
docker compose run --rm lakehouse python -m lakehouse.jobs.run_bronze --env dev --datasets timelines --max-files 500 --batch-size 25
```

### Silver

Chạy toàn bộ Silver:

```bash
docker compose run --rm lakehouse python -m lakehouse.jobs.run_silver --env dev
```

Chạy theo dataset:

```bash
docker compose run --rm lakehouse python -m lakehouse.jobs.run_silver --env dev --datasets matches,timelines
```

Chạy theo bảng:

```bash
docker compose run --rm lakehouse python -m lakehouse.jobs.run_silver --env dev --tables matches,participants,teams
```

### Gold

Chạy toàn bộ Gold:

```bash
docker compose run --rm lakehouse python -m lakehouse.jobs.run_gold --env dev
```

Chạy một số bảng Gold:

```bash
docker compose run --rm lakehouse python -m lakehouse.jobs.run_gold --env dev --tables dim_summoner,mart_player_daily_performance
```

### Platinum

Chạy Platinum feature registry:

```bash
docker compose run --rm lakehouse python -m lakehouse.jobs.run_platinum --env dev
```

Job hiện tại in ra SQL registry cho feature layer. Có thể mở rộng bước này để materialize feature tables ra `data/lakehouse/platinum/` hoặc S3.

### Data Quality

Chạy data quality cho Silver và Gold:

```bash
docker compose run --rm lakehouse python -m lakehouse.jobs.run_data_quality --env dev
```

Chạy riêng Gold:

```bash
docker compose run --rm lakehouse python -m lakehouse.jobs.run_data_quality --env dev --layers gold
```

Fail pipeline nếu có error-level check:

```bash
docker compose run --rm lakehouse python -m lakehouse.jobs.run_data_quality --env dev --fail-on-error
```

Report được ghi vào:

```text
reports/data_quality/data_quality_latest.json
reports/data_quality/data_quality_latest.md
```

## Chạy Local Không Dùng Docker

Cài dependencies:

```bash
pip install -e ".[dev,spark,aws]"
```

Chạy từng job:

```bash
python -m lakehouse.jobs.run_bronze --env dev
python -m lakehouse.jobs.run_silver --env dev
python -m lakehouse.jobs.run_gold --env dev
python -m lakehouse.jobs.run_data_quality --env dev
```

## Chạy Airflow

Start Airflow webserver và scheduler:

```bash
docker compose --profile airflow up --build airflow
```

Mở Airflow UI:

```text
http://localhost:8088
```

Nếu bạn bỏ trống `AIRFLOW_PORT`, Docker Compose sẽ dùng default `8080`.

Thông tin đăng nhập mặc định trong `.env.example`:

```text
username: admin
password: admin
```

Các DAG chính:

| DAG | Mục đích |
| --- | --- |
| `riot_bronze_ingestion` | Chạy Bronze ingestion |
| `riot_silver_transform` | Chạy Silver transform |
| `riot_gold_model` | Chạy Gold transform |
| `riot_platinum_features` | Chạy Platinum feature registry |
| `riot_full_lakehouse_pipeline` | Chạy Bronze -> Silver -> Gold -> Platinum -> Data Quality |

Airflow metadata local được lưu trong `metadata/airflow/`.

## AWS S3, Glue Và Athena

### Cấu Hình S3

Tạo file `.env.prod` hoặc cập nhật `.env`:

```env
LAKEHOUSE_ENV=prod
LAKEHOUSE_ENV_FILE=.env.prod

AWS_REGION=ap-southeast-1
AWS_ACCESS_KEY_ID=<your-access-key>
AWS_SECRET_ACCESS_KEY=<your-secret-key>
AWS_SESSION_TOKEN=

S3_BUCKET=<your-riot-lakehouse-bucket>
S3_RAW_PREFIX=raw
S3_LAKEHOUSE_PREFIX=lakehouse
S3_CHECKPOINT_PREFIX=metadata/checkpoints
S3_REPORT_PREFIX=reports
ATHENA_DATABASE=riot_lakehouse
```

Chạy pipeline với config production:

```powershell
$env:LAKEHOUSE_ENV_FILE=".env.prod"
docker compose --env-file .env.prod run --rm lakehouse python -m lakehouse.jobs.run_full_pipeline --env prod
```

Hoặc:

```bash
docker compose --env-file .env.prod run --rm lakehouse python -m lakehouse.jobs.run_full_pipeline --env prod
```

`configs/prod.yaml` sẽ map các root path sang S3:

```text
raw_root        -> s3://<bucket>/<raw-prefix>
lakehouse_root  -> s3://<bucket>/<lakehouse-prefix>
checkpoint_root -> s3://<bucket>/<checkpoint-prefix>
report_root     -> s3://<bucket>/<report-prefix>
```

### Glue Data Catalog

Athena dùng Glue Data Catalog làm metadata catalog. SQL DDL templates nằm tại:

```text
sql/athena/create_bronze_tables.sql
sql/athena/create_silver_tables.sql
sql/athena/create_gold_tables.sql
sql/athena/create_platinum_tables.sql
```

Các file SQL này là schema templates. Khi chạy thực tế trên Athena, hãy bổ sung `LOCATION` tương ứng với S3 output của từng bảng, ví dụ:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.silver_matches (
  match_id string,
  game_creation bigint
)
PARTITIONED BY (dataset string, game_date string)
STORED AS PARQUET
LOCATION 's3://<bucket>/lakehouse/silver/matches/';
```

Sau khi tạo table partitioned, chạy:

```sql
MSCK REPAIR TABLE riot_lakehouse.silver_matches;
```

Hoặc dùng Glue Crawler nếu muốn tự động phát hiện partition.

### Athena Query Ví Dụ

```sql
SELECT
  game_date,
  champion_id,
  champion_name,
  matches_played,
  win_rate
FROM riot_lakehouse.gold_mart_champion_daily_performance
ORDER BY game_date DESC, matches_played DESC
LIMIT 50;
```

```sql
SELECT
  game_date,
  puuid,
  summoner_name,
  matches_played,
  avg_kda
FROM riot_lakehouse.gold_mart_player_daily_performance
ORDER BY game_date DESC, matches_played DESC;
```

## Power BI Dashboard

Khuyến nghị kết nối Power BI theo hướng:

1. Publish dữ liệu Lakehouse lên S3.
2. Tạo Glue/Athena tables cho Gold marts.
3. Cấu hình Athena query result location, ví dụ `s3://<bucket>/athena-results/`.
4. Kết nối Power BI tới Athena bằng Amazon Athena connector hoặc ODBC driver.
5. Import/DirectQuery các bảng Gold:
   - `gold_mart_player_daily_performance`
   - `gold_mart_champion_daily_performance`
   - `gold_mart_role_daily_performance`
   - `gold_mart_rank_daily_summary`
   - `gold_mart_team_objective_daily_summary`

Dashboard gợi ý:

- Tổng số match theo ngày
- Top champion theo win rate và pick rate
- Player performance: KDA, damage, vision score, gold earned
- Role performance theo lane/team position
- Ranked distribution theo tier/rank
- Objective control: dragon, baron, tower
- Data Quality Score theo ngày chạy pipeline

Ảnh dashboard có thể đặt tại:

```text
docs/images/powerbi-dashboard.png
```

Và chèn vào README:

```md
![Power BI Dashboard](docs/images/powerbi-dashboard.png)
```

## Testing

Chạy toàn bộ test:

```bash
pytest -q
```

Chạy test bằng Docker:

```bash
docker compose run --rm lakehouse pytest -q
```

Chạy một file test:

```bash
pytest tests/test_silver_transform.py -q
```

Test suite hiện bao phủ:

- Config loading
- Raw dataset detection
- Checkpoint
- Bronze ingestion
- Silver transform
- Gold aggregation/transformer
- Platinum features
- Data quality rules
- Airflow DAG imports

Lint:

```bash
ruff check src tests dags
```

## CI/CD

Workflow hiện có tại `.github/workflows/ci.yml`:

```yaml
name: ci

on:
  push:
  pull_request:

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install -e ".[dev]"
      - run: ruff check src tests dags
      - run: pytest -q
```

Có thể mở rộng CI/CD production theo các bước:

1. Lint và test.
2. Build Docker image.
3. Push image lên container registry.
4. Deploy Airflow DAGs.
5. Deploy Spark jobs hoặc container job.
6. Chạy smoke test trên môi trường staging.
7. Gửi notification khi pipeline fail.

Các secrets nên quản lý trong GitHub Actions Secrets:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- `S3_BUCKET`
- `RIOT_API_KEY`

## Monitoring Và Data Quality

Monitoring được thiết kế theo nhiều lớp:

| Layer | Signal |
| --- | --- |
| Airflow | DAG status, task duration, retries, SLA miss |
| Spark | Job runtime, row count, partition count, failed stages |
| S3 | Object count, data size, partition freshness |
| Data Quality | Pass/fail rules, quality score, null rate, duplicate rate |
| Athena/BI | Query failures, dashboard freshness |
| CloudWatch | Logs, metrics, alarms khi chạy trên AWS |

Data quality report được sinh bởi:

```bash
docker compose run --rm lakehouse python -m lakehouse.jobs.run_data_quality --env dev
```

Report Markdown mới nhất:

```text
reports/data_quality/data_quality_latest.md
```

Các rule tiêu biểu:

- Table existence
- Row count
- Required columns
- Not-null checks
- Unique business key
- Non-negative metrics
- Win-rate range
- Aggregate consistency

## Mở Rộng Project

### Thêm Dataset Mới

Ví dụ thêm `spectator`, `league` hoặc `static`:

1. Thêm raw folder: `raw/spectator/` hoặc `raw/static/`.
2. Cập nhật dataset detection trong `src/lakehouse/raw/detect_dataset.py`.
3. Cập nhật manifest trong `src/lakehouse/raw/raw_manifest.py`.
4. Thêm cleaner trong `src/lakehouse/silver/`.
5. Cập nhật `SILVER_TABLES` và schema trong Silver transformer.
6. Thêm metadata vào `configs/tables.yaml`.
7. Thêm Gold aggregation nếu dataset phục vụ BI.
8. Thêm Athena DDL tương ứng.
9. Thêm pytest cho raw detection, transform và quality rules.

### Bật Delta Lake

Project hiện dùng Parquet mặc định. Có thể bật Delta Lake bằng config:

```env
SPARK_ENABLE_DELTA=true
```

Khi bật Delta Lake trong production, cần bổ sung package/runtime Delta tương thích với Spark version và cập nhật writer nếu muốn ghi Delta thay vì Parquet.

### Production Hardening

Các hướng nâng cấp phù hợp cho môi trường production:

- Secrets Manager hoặc Parameter Store thay cho `.env`.
- IAM role thay cho static AWS keys.
- Glue Crawler hoặc catalog registration tự động.
- Airflow retries, SLA và alerting.
- Great Expectations hoặc Deequ cho data quality nâng cao.
- Iceberg/Delta/Hudi cho ACID table format.
- Incremental Silver/Gold theo watermark.
- Backfill workflow theo date range hoặc match range.
- Data contract cho từng Riot API endpoint.

## Chèn Ảnh Vào README

Ảnh architecture nên đặt trong repo để GitHub render ổn định:

```text
docs/images/Pipeline.png
```

Markdown đã dùng ở đầu README:

```md
![Data Lakehouse Architecture](docs/images/Pipeline.png)
```

Nếu muốn thêm dashboard:

```text
docs/images/powerbi-dashboard.png
```

```md
![Power BI Dashboard](docs/images/powerbi-dashboard.png)
```

## Bảo Mật

- Không commit `.env`, API key hoặc AWS credentials.
- Raw JSON có thể chứa dữ liệu nhạy cảm hoặc dữ liệu bị giới hạn bởi Terms of Service, cần kiểm tra trước khi public.
- Nên rotate Riot API key và AWS keys định kỳ.
- Khi mở source trên GitHub, chỉ commit sample data nhỏ đã được kiểm tra.

## Trạng Thái Hiện Tại

- Bronze/Silver/Gold jobs: có CLI entrypoint và test coverage.
- Platinum: có SQL feature registry, sẵn sàng mở rộng materialization.
- Airflow: có DAG riêng từng layer và DAG full pipeline.
- Data Quality: sinh JSON/Markdown report cho Silver/Gold.
- AWS: hỗ trợ S3 path, Athena DDL templates và Glue/Athena integration path.
- Crawler: contract đầu ra là raw JSON trong `raw/{dataset}/*.json`; có thể đặt crawler như service upstream hoặc bổ sung module crawler trong repo.
