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

# Đọc parquet từ bronze 
df_cust = spark.read \
    .option("header", True) \
    .parquet("s3a://bronze/crm/cust/dt=20260430/")

df_cust.show()
print ("tổng số lượng dòng trong df_cust: ", df_cust.count())

# lọc null cst_id không null
# lọc cst_key không null
# ranking theo thông tin khách hàng mới nhất created date
# convert cst_marital_status với giá trị Married và Single
# cst_gndr với giá trị Male và Female
# convert kiểu dữ liệu cho chuẩn lại  
# printSchema để check kiểu dữ liệu đã đúng chưa






