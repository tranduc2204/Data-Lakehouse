# ✔ 1. Connect SQL Server
# ✔ 2. Decide FULL vs INCREMENTAL
# ✔ 3. Write raw data vào MinIO (Parquet)

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
import os
import bronze_check_valid as validator 
import shutil

INCOMING_PATH = "./data/landing/incoming"
PROCESSED_PATH = "./data/landing/processed"


spark = SparkSession.builder \
    .appName("CSV_to_MinIO") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "12345678") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()


def bronze_check_validator (file_path):
    if not validator.validate_file(file_path):
        print(f"Skipping file: {file_path}")
        return False
    return True

def parse_file_name(file_name):
    parts = file_name.replace(".csv", "").split("_")
    
    source = parts[0]
    table = parts[1]
    date = parts[2]  # 20260430
    time = parts[3]  # 080000

    return source, table, date, time


def move_to_processed(file_name):
    src = os.path.join(INCOMING_PATH, file_name)
    dst = os.path.join(PROCESSED_PATH, file_name)

    # tạo folder nếu chưa có
    os.makedirs(PROCESSED_PATH, exist_ok=True)

    shutil.move(src, dst)

    print(f"📦 Moved {file_name} → processed/")


# #overwrite tạm thời dùng mode overwrite thay vì append
def load_to_bronze(file_path):
    bronze_check_validator(file_path)
    file_name = os.path.basename(file_path)
    source, table, date, time = parse_file_name(file_name)
    table_name = "_".join([source, table])
    # table_name = os.path.splitext(file_name)[0].split("_")[:2]  # cust_info  



 

    print ("\n\n\n")
    print (table_name, "  ", file_name)

    print ("\n\n\n") 
    df = spark.read.option("header", True).csv(file_path)

    # thêm metadata
    df = df.withColumn("load_date", current_date())

    df.write \
        .mode("append") \
        .partitionBy("load_date") \
        .parquet(f"s3a://bronze/{source}/{table}/dt={date}/")
        # .parquet(f"s3a://bronze/{table_name}/") #parquet
        
    # move data từ xử lý sang mục đã xử lý
    move_to_processed(file_name)
    print(f"✅ Loaded to bronze/{source}/{table}/dt={date}/")



if __name__ == "__main__":
   
    load_to_bronze("./data/landing/incoming/crm_cust_20260430_080000.csv")




    load_to_bronze("./data/landing/incoming/crm_prd_20260430_080000.csv")
    load_to_bronze("./data/landing/incoming/crm_sales_20260430_080000.csv")
    load_to_bronze("./data/landing/incoming/erp_CAT_20260430_080000.csv")
    load_to_bronze("./data/landing/incoming/erp_cust_20260430_080000.csv")
    load_to_bronze("./data/landing/incoming/erp_LOC_20260430_080000.csv")
