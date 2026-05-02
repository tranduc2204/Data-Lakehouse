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
# df_sales = spark.read \
#     .option("header", True) \
#     .parquet("s3a://bronze/crm/sales/dt=20260430/")

# df_sales.show()
# print ("tổng số lượng dòng trong df_sales: ", df_sales.count())


# check valid của sls_order_dt, sls_ship_dt, sls_due_dt
# check lại quantity



def silver_crm_sales(process_date):

    # đọc parquet từ Bronze
    df_sales = spark.read \
        .option("header", True) \
        .parquet(f"s3a://bronze/crm/sales/dt={process_date}/")

 

    
    # check kiểu dữ liệu cho chuẩn
    df_sales = df_sales.withColumn(
        "sls_order_dt",
        when(
            (col("sls_order_dt").isNull()) |
            (length(col("sls_order_dt")) != 8) |
            (col("sls_order_dt") == 0),
            None
        ).otherwise(col("sls_order_dt"))
    ).withColumn(
        "sls_ship_dt",
        when(
            (col("sls_ship_dt").isNull()) |
            (length(col("sls_ship_dt")) != 8) |
            (col("sls_ship_dt") == 0),
            None
        ).otherwise(col("sls_ship_dt"))
    ).withColumn(
        "sls_due_dt",
        when(
            (col("sls_due_dt").isNull()) |
            (length(col("sls_due_dt")) != 8) |
            (col("sls_due_dt") == 0),
            None
        ).otherwise(col("sls_due_dt"))
    ).withColumn('sls_quantity', abs(col('sls_quantity')))\
    .withColumn('sls_sales', abs(col('sls_price')) * col('sls_quantity'))\
    .withColumn('sls_price', abs(col('sls_price')))

    return df_sales


def run (process_date): 
    df = silver_crm_sales(process_date)

    output_path = f"s3a://silver/crm/sales/dt={process_date}/"
    df.write.mode("overwrite").parquet(output_path)




if __name__ == "__main__":
    process_date = 20260430
    run(process_date)
    print ('done')



















