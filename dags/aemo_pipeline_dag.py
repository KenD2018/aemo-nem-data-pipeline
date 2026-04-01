"""
AEMO NEM Data Pipeline DAG
===========================
作用：从澳洲能源市场运营商（AEMO）自动下载电力价格和需求数据
     存入GCS数据湖 → 加载到BigQuery → 创建分区+聚簇优化表

作者：KD（DE Zoomcamp Final Project）
日期：2026年3月

面试解释：
这个DAG做了4件事：
1. 从AEMO官网批量下载300个CSV文件（5个州 × 60个月）
2. 把原始CSV文件上传到GCS（数据湖），作为原始数据备份
3. 把GCS里的文件批量加载到BigQuery原始表
4. 把原始表转成按日期分区、按州聚簇的优化表，方便分析查询
"""

import os
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


# ============================================================
# 配置区 — 根据你的GCP项目修改这里
# ============================================================
GCP_PROJECT_ID = "taxi-rides-ny-v2"               # 你的GCP项目ID（service account密钥所属项目）
GCP_BUCKET = "aemo-nem-data-lake-v2"      # 新建的GCS bucket名字
BIGQUERY_DATASET = "aemo_electricity"     # BigQuery dataset名字
BIGQUERY_TABLE_RAW = "raw_nem_data"       # 原始表名
BIGQUERY_TABLE_FINAL = "nem_partitioned"  # 分区+聚簇优化表名

# AEMO数据的5个州代码
REGIONS = ["NSW1", "VIC1", "QLD1", "SA1", "TAS1"]

# 下载时间范围：2020年1月 到 2024年12月
START_YEAR, START_MONTH = 2020, 1
END_YEAR, END_MONTH = 2024, 12

# 本地临时存储路径（在Airflow容器内）
LOCAL_STORAGE_PATH = "/tmp/aemo_data"

# AEMO数据URL前缀
AEMO_BASE_URL = "https://aemo.com.au/aemo/data/nem/priceanddemand"


# ============================================================
# Task 1：下载AEMO CSV文件到本地
# ============================================================
def download_aemo_data(**context):
    """
    从AEMO官网批量下载CSV文件，保存到容器本地的/tmp/aemo_data/目录。

    为什么这样做：
    - AEMO的URL有固定规律：PRICE_AND_DEMAND_{年月}_{州}.csv
    - 用两层循环（时间 × 州）自动生成所有300个文件的URL
    - 先存本地，再上传GCS，可以在中途失败时断点续传
    """
    os.makedirs(LOCAL_STORAGE_PATH, exist_ok=True)

    downloaded = 0
    failed = 0

    # 生成2020-01到2024-12的所有月份
    current = datetime(START_YEAR, START_MONTH, 1)
    end_date = datetime(END_YEAR, END_MONTH, 1)

    while current <= end_date:
        year_month = current.strftime("%Y%m")  # 例如：202001

        for region in REGIONS:
            filename = f"PRICE_AND_DEMAND_{year_month}_{region}.csv"
            local_path = f"{LOCAL_STORAGE_PATH}/{filename}"

            # 如果文件已存在就跳过（支持断点续传）
            if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                print(f"[跳过] 已存在: {filename}")
                downloaded += 1
                continue

            url = f"{AEMO_BASE_URL}/{filename}"

            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()

                with open(local_path, "wb") as f:
                    f.write(response.content)

                print(f"[成功] 下载: {filename}")
                downloaded += 1

            except Exception as e:
                print(f"[失败] {filename}: {e}")
                failed += 1
                # 继续处理其他文件，不中断整个任务

        current += relativedelta(months=1)

    print(f"\n下载完成：成功 {downloaded} 个，失败 {failed} 个")

    if downloaded == 0:
        raise Exception("没有下载到任何文件，请检查网络或AEMO URL是否有效")


# ============================================================
# Task 2：把本地CSV文件上传到GCS
# ============================================================
def upload_to_gcs(**context):
    """
    把本地/tmp/aemo_data/里的所有CSV文件上传到GCS。

    为什么用GCS作为数据湖：
    - GCS是廉价的对象存储（类似云端硬盘），适合存原始文件
    - 保留原始数据，即使BigQuery表出问题也能重新加载
    - 这是数据工程里的标准模式：原始数据永远不删
    """
    from google.cloud import storage

    client = storage.Client(project=GCP_PROJECT_ID)
    bucket = client.bucket(GCP_BUCKET)

    uploaded = 0
    files = [f for f in os.listdir(LOCAL_STORAGE_PATH) if f.endswith(".csv")]

    print(f"准备上传 {len(files)} 个文件到 gs://{GCP_BUCKET}/raw/")

    for filename in files:
        local_path = f"{LOCAL_STORAGE_PATH}/{filename}"
        gcs_path = f"raw/{filename}"  # 在GCS里存放在raw/文件夹下

        blob = bucket.blob(gcs_path)

        # 如果GCS里已存在就跳过
        if blob.exists():
            print(f"[跳过] 已存在于GCS: {gcs_path}")
            uploaded += 1
            continue

        blob.upload_from_filename(local_path)
        print(f"[成功] 上传: {filename} → gs://{GCP_BUCKET}/{gcs_path}")
        uploaded += 1

    print(f"\n上传完成：共 {uploaded} 个文件")


# ============================================================
# Task 3：从GCS批量加载数据到BigQuery原始表
# ============================================================
def load_gcs_to_bigquery(**context):
    """
    把GCS里所有CSV文件一次性加载到BigQuery的原始表。

    为什么用WRITE_TRUNCATE（覆盖写）：
    - 每次重新运行pipeline时，确保数据不重复
    - 原始表只是中间层，后续会转成分区表
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT_ID)

    # GCS里所有AEMO CSV文件的路径（用*通配符匹配所有文件）
    gcs_uri = f"gs://{GCP_BUCKET}/raw/PRICE_AND_DEMAND_*.csv"
    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE_RAW}"

    job_config = bigquery.LoadJobConfig(
        autodetect=True,                                          # 自动检测列类型
        skip_leading_rows=1,                                      # 跳过CSV标题行
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # 覆盖写（避免重复）
        source_format=bigquery.SourceFormat.CSV,
    )

    print(f"开始加载数据: gs://{GCP_BUCKET}/raw/ → {table_id}")
    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()  # 等待BigQuery任务完成

    table = client.get_table(table_id)
    print(f"加载完成！原始表共有 {table.num_rows:,} 行数据")


# ============================================================
# Task 4：创建分区+聚簇的优化表（面试必考）
# ============================================================
def create_partitioned_table(**context):
    """
    把原始表转成按日期分区、按州聚簇的优化表。

    面试必答知识点：

    分区（Partitioning）：
    - 把大表按时间切成小块，查询时只扫描需要的分区
    - 例如查"2023年的数据"，BigQuery只读2023年那块，不扫描其他年份
    - 省钱：BigQuery按扫描量收费，分区能大幅减少扫描量

    聚簇（Clustering）：
    - 在分区内，把相同州的数据物理上存在一起
    - 按州筛选时BigQuery可以跳过其他州的数据块
    - 本项目按REGIONID聚簇，因为很多查询都会筛选特定州
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT_ID)

    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE_FINAL}"

    sql = f"""
    CREATE OR REPLACE TABLE `{table_id}`
    PARTITION BY DATE(settlement_date)
    CLUSTER BY region_id
    AS
    SELECT
        -- 时间列：从字符串转成TIMESTAMP（AEMO格式：2020/01/01 00:05:00）
        SETTLEMENTDATE AS settlement_date,

        -- 州代码（NSW1, VIC1, QLD1, SA1, TAS1）
        REGION AS region_id,

        -- 总用电需求（单位：兆瓦MW）
        SAFE_CAST(TOTALDEMAND AS FLOAT64) AS total_demand_mw,

        -- 地区参考价格（单位：澳元/兆瓦时）
        SAFE_CAST(RRP AS FLOAT64) AS price_aud_per_mwh,

        -- 时段类型
        PERIODTYPE AS period_type

    FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE_RAW}`
    WHERE SETTLEMENTDATE IS NOT NULL
      AND REGION IN ('NSW1', 'VIC1', 'QLD1', 'SA1', 'TAS1')
    """

    print(f"创建分区+聚簇表: {table_id}")
    query_job = client.query(sql)
    query_job.result()

    table = client.get_table(table_id)
    print(f"优化表创建成功！共 {table.num_rows:,} 行，"
          f"按 settlement_date 分区，按 region_id 聚簇")


# ============================================================
# DAG定义：把4个Task串起来
# ============================================================
with DAG(
    dag_id="aemo_nem_data_pipeline",
    description="澳洲电力市场数据管道 - DE Zoomcamp Final Project",
    schedule_interval="@monthly",
    start_date=datetime(2020, 1, 1),
    catchup=False,        # 不补跑历史，我们手动触发一次
    max_active_runs=1,    # 同时只跑一个实例，避免冲突
    tags=["aemo", "final-project", "gcp"],
) as dag:

    # Task 1：下载
    t1_download = PythonOperator(
        task_id="download_aemo_data",
        python_callable=download_aemo_data,
        execution_timeout=None,  # 下载300个文件可能需要较长时间，不设超时
    )

    # Task 2：上传GCS
    t2_upload_gcs = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
    )

    # Task 3：加载BigQuery
    t3_load_bq = PythonOperator(
        task_id="load_gcs_to_bigquery",
        python_callable=load_gcs_to_bigquery,
    )

    # Task 4：创建优化表
    t4_partition = PythonOperator(
        task_id="create_partitioned_table",
        python_callable=create_partitioned_table,
    )

    # 依赖链：顺序执行
    # 下载 → 上传GCS → 加载BigQuery → 创建分区表
    t1_download >> t2_upload_gcs >> t3_load_bq >> t4_partition
