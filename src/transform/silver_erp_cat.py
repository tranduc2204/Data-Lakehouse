from pyspark.sql.functions import * 
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from minio import Minio
from minio.error import S3Error


# kết nối MinIO
client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="12345678",
    secure=False
)

bucket_name = "silver"
try:
    # check tồn tại chưa
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"✅ Bucket '{bucket_name}' created")
    else:
        print(f"⚠️ Bucket '{bucket_name}' already exists")

except S3Error as err:
    print("❌ Error:", err)



spark = SparkSession.builder \
    .appName("Read_From_MinIO") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "12345678") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()


# # đọc parquet từ Bronze
# df_cat = spark.read \
#     .option("header", True) \
#     .parquet("s3a://bronze/erp/CAT/dt=20260430/")

# df_cat.show()
# print ("tổng số lượng dòng trong df_cat: ", df_cat.count())


# check NULL ID
# check upper ID
# check CAT trim và upper
# check mainenance upper trim 

def silver_erp_cat (process_date):

    # đọc parquet từ Bronze
    df_cat = spark.read \
        .option("header", True) \
        .parquet(f"s3a://bronze/erp/CAT/dt={process_date}/")

    # df_cat.show()
    # print ("tổng số lượng dòng trong df_cat: ", df_cat.count())
    df_cat = df_cat.filter(col('ID').isNotNull())

    df_cat = df_cat\
                .withColumn('CAT', upper(trim(col('CAT'))))\
                .withColumn('MAINTENANCE', upper(trim(col('MAINTENANCE'))))\
                .withColumn('ID', upper(trim(col('ID'))))
    return df_cat

def run (process_date):
    df = silver_erp_cat (process_date)

    output_path = f"s3a://silver/erp/cat/dt={process_date}/"
    df.write.mode("overwrite").parquet(output_path)


if __name__ == "__main__":
    process_date = 20260430
    run (process_date)






















