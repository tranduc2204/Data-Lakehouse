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
# df_erp_cust = spark.read \
#     .option("header", True) \
#     .parquet("s3a://bronze/erp/cust/dt=20260430/")

# df_erp_cust.show()
# print ("tổng số lượng dòng trong df_erp_cust: ", df_erp_cust.count())

# check CID is not null 
# check BDATE convert sang 1 kiểu date time nhất định
# check gen check cho nó chuẩn hóa lại 
# check load_date convert sang 1 kiểu date nhất định


def silver_erp_cust (process_date):
    # đọc parquet từ Bronze
    df_erp_cust = spark.read \
        .option("header", True) \
        .parquet(f"s3a://bronze/erp/cust/dt={process_date}/")
    
    # df_erp_cust.show()
    
    # print ("tổng số lượng dòng trong df_erp_cust: ", df_erp_cust.count())
    df_erp_cust = df_erp_cust.filter(col('CID').isNotNull())
    # df_erp_cust = df_erp_cust.withColumn('CID', when (col('CID').like('NAS%'), substring(col("CID"), 4, length(col("cid"))).otherwise(col('CID'))))\



    df_erp_cust = df_erp_cust.withColumn(
                                        "CID",
                                        when(
                                            col("CID").startswith("NAS"),
                                            substring(col("CID"), 4, 100)
                                        ).otherwise(col("CID"))
                                    )\
                        .withColumn('BDATE', to_date(col('BDATE'), 'yyyy-MM-dd'))\
                        .withColumn('GEN', upper(trim(col('GEN'))))\
                        .withColumn('load_date', to_date(col('load_date'), 'yyyy-MM-dd'))
    df_erp_cust.show()
    return df_erp_cust
    


def run (process_date):
    df = silver_erp_cust (process_date)

    output_path = f"s3a://silver/erp/cust/dt={process_date}/"
    df.write.mode("overwrite").parquet(output_path)


if __name__ == "__main__":

    process_date = 20260430
    
    run (process_date)
    













