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
df_erp_cust = spark.read \
    .option("header", True) \
    .parquet("s3a://bronze/erp/cust/dt=20260430/")

df_erp_cust.show()
print ("tổng số lượng dòng trong df_erp_cust: ", df_erp_cust.count())

# check CID is not null 
# check BDATE convert sang 1 kiểu date time nhất định
# check gen check cho nó chuẩn hóa lại 
# check load_date convert sang 1 kiểu date nhất định










