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


# đọc parquet từ Bronze
# df_prd = spark.read \
#     .option("header", True) \
#     .parquet("s3a://bronze/crm/prd/dt=20260430/")

# df_prd.show()
# print ("tổng số lượng dòng trong df_prd: ", df_prd.count())


# check null ID 
# chuẩn hóa lại prd_key --> uppeer
# chuẩn hóa prd_nm -- trim
# convert kiểu dữ liệu datetime
# prd_start_dt và prd_end_dt đang sai cần update lại pred_end_dt


def silver_crm_prd (process_date):
    # đọc parquet từ Bronze
    df_prd = spark.read \
        .option("header", True) \
        .parquet(f"s3a://bronze/crm/prd/dt={process_date}/")

    
    # check NULL ID
    df_prd = df_prd.filter(col('prd_id').isNotNull())
   
    # chủâ hóa format date lại
    df_prd = df_prd.withColumn('prd_start_dt', to_date(col('prd_start_dt'), 'yyyy-MM-dd'))\
            .withColumn('prd_end_dt', to_date(col('prd_end_dt'), 'yyyy-MM-dd'))
   
    # prd_start_dt và prd_end_dt đang sai cần update lại pred_end_dt
    windowSpec = Window.partitionBy(col('prd_key')).orderBy(col('prd_id'))
    
    df_prd = df_prd.withColumn('prd_end_dt', lead('prd_start_dt',1).over(windowSpec))#.filter(col('prd_key') == 'AC-HE-HL-U509-R').show(5)

    df_prd = df_prd.withColumn('prd_line', when(trim(col('prd_line')) == 'M', 'Mountain')\
                            .when(trim(col('prd_line')) == 'R', 'Road')\
                            .when(trim(col('prd_line')) == 'S', 'Other Sales')\
                            .when(trim(col('prd_line')) == 'T', 'Touring')\
                            .otherwise ('N/a'))\
                    .withColumn('prd_nm', trim(col('prd_nm')))\
                    .withColumn('prd_key', trim(regexp_replace(substring(col('prd_key'), 1,5),'-','_')))
    df_prd.show()

    return df_prd

def run(process_date):
    df = silver_crm_prd (process_date)
    # df.show()
    output_path = f"s3a://silver/crm/prd/dt={process_date}/"
    df.write.mode("overwrite").parquet(output_path)

if __name__ == '__main__':
    process_date = 20260430
    run (process_date)




















