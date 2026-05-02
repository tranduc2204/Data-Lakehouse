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

# Đọc parquet từ bronze 
# df_cust = spark.read \
#     .option("header", True) \
#     .parquet("s3a://bronze/crm/cust/dt=20260430/")

# df_cust.show()
# print ("tổng số lượng dòng trong df_cust: ", df_cust.count())

# lọc null cst_id không null
# lọc cst_key không null
# ranking theo thông tin khách hàng mới nhất created date
# convert cst_marital_status với giá trị Married và Single
# cst_gndr với giá trị Male và Female
# convert kiểu dữ liệu cho chuẩn lại  
# printSchema để check kiểu dữ liệu đã đúng chưa



def silver_crm_cust (process_date):

    df_cust = spark.read \
    .option("header", True) \
    .parquet(f"s3a://bronze/crm/cust/dt={process_date}/") #s3a://bronze/crm/cust/dt=20260430/

    #check số lượng sau khi lọc id null
    df_cust = df_cust.filter(col('cst_id').isNotNull())

    # check số lượng customer key null
    df_cust.filter(col('cst_key').isNull()).count()
    df_cust = df_cust.filter(col('cst_key').isNotNull())


    # ranking theo thông tin khách hàng mới nhất created date
    windowSpec = Window.partitionBy("cst_key").orderBy(col('cst_create_date').desc())
    # df_cust.withColumn("rk",row_number().over(windowSpec)).filter(col("cst_key") == 'AW00029433')
    # chỉ lọc lấy giá trị up sau và bỏ giá trị trước vì có thể bị sai
    df_cust = df_cust.withColumn('rk', row_number().over(windowSpec)).filter(col('rk') == 1).drop('rk')
  
    # cst_gndr với giá trị Male và Female
    # convert kiểu dữ liệu cho chuẩn lại

    df_cust = df_cust.withColumn('cst_marital_status', when(trim(col('cst_marital_status')) == 'M', 'Married')\
                                    .when(trim(col('cst_marital_status')) == 'S', 'Single')\
                                    .otherwise('N/a'))
    df_cust = df_cust.withColumn('cst_gndr', when(trim(col('cst_gndr')) == 'M', 'Male')\
                                    .when(trim(col('cst_gndr')) == 'F', 'Female')\
                                    .otherwise('N/a'))

    df_cust = df_cust.withColumn('cst_create_date', to_date(col('cst_create_date'), 'yyyy-MM-dd'))

    df_cust.show()
    return df_cust
    
def run(process_date): #input_path
    df = silver_crm_cust (process_date)
    # df.show()
    output_path = f"s3a://silver/crm/cust/dt={process_date}/"
    df.write.mode("overwrite").parquet(output_path)

if __name__ == '__main__':
    process_date = 20260430
    run (process_date)

    


















