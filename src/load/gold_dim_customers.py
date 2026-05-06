import pandas
from pyspark.sql.functions import * 
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from minio import Minio
from minio.error import S3Error

def gold_dim_customers(process_date):
    # kết nối MinIO
    client = Minio(
        "localhost:9000",
        access_key="admin",
        secret_key="12345678",
        secure=False
    )

    bucket_name = "gold"
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
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.1,org.postgresql:postgresql:42.7.3"
        ) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "12345678") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    df_crm_cust = spark.read \
        .option("header", True) \
        .parquet(f"s3a://silver/crm/cust/dt={process_date}/")


    df_erp_cust = spark.read \
        .option("header", True) \
        .parquet(f"s3a://silver/erp/cust/dt={process_date}/")
        
    df_erp_loc = spark.read \
        .option("header", True) \
        .parquet(f"s3a://silver/erp/loc/dt={process_date}/")




    df_dim_customers = df_crm_cust.alias("crm_c").join(df_erp_cust.alias("erp_c"), col("crm_c.cst_key") == col("erp_c.CID") , "left")\
                .join(df_erp_loc.alias("erp_l"), col("crm_c.cst_key") == col("erp_l.CID"), "left")\
                .select(
                    col("crm_c.cst_id"),\
                    col("crm_c.cst_key"),\
                    col("crm_c.cst_firstname"),\
                    col("crm_c.cst_lastname"),\
                    col("erp_l.CNTRY"),\
                    col("crm_c.cst_marital_status"),\
                    # when (col("crm_c.cst_gndr") != "N/a", col("crm_c.cst_gndr")).otherwise(col("erp.GEN"))
                    when(
                        col("crm_c.cst_gndr") != "N/a",
                        col("crm_c.cst_gndr")
                    ).otherwise(col("erp_c.GEN")).alias("cst_gndr"),\
                    col("erp_c.bdate"),\
                    col("crm_c.cst_create_date"),\
                )
    return df_dim_customers





#  

def run (process_date): 
    df = gold_dim_customers(process_date)
    df.show()
    output_path = f"s3a://gold/dim_customers/dt={process_date}/"
    df.write.mode("overwrite").parquet(output_path)

    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/warehouse")\
        .option("dbtable", "dwh.dim_customers") \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()


if __name__ == "__main__":
    process_date = 20260430
    run(process_date)
    print ('done')













