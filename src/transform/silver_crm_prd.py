from pyspark.sql.functions import * 
from pyspark.sql.window import Window
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("Read_From_MinIO") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "12345678") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()


# đọc parquet từ Bronze
df_prd = spark.read \
    .option("header", True) \
    .parquet("s3a://bronze/crm/prd/dt=20260430/")

df_prd.show()
print ("tổng số lượng dòng trong df_prd: ", df_prd.count())


# check null ID 
# chuẩn hóa lại prd_key --> uppeer
# chuẩn hóa prd_nm -- trim
# convert kiểu dữ liệu datetime
# prd_start_dt và prd_end_dt đang sai cần update lại pred_end_dt






























