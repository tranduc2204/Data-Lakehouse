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
df_cat = spark.read \
    .option("header", True) \
    .parquet("s3a://bronze/erp/CAT/dt=20260430/")

df_cat.show()
print ("tổng số lượng dòng trong df_cat: ", df_cat.count())


# check NULL ID
# check upper ID
# check CAT trim và upper
# check mainenance upper trim 

























