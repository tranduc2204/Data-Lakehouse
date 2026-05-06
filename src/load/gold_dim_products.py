import pandas
from pyspark.sql.functions import * 
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from minio import Minio
from minio.error import S3Error


def gold_dim_products(process_date):
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
  

    df_crm_prd = spark.read \
        .option("header", True) \
        .parquet(f"s3a://silver/crm/prd/dt={process_date}/")
    df_erp_cat = spark.read \
        .option("header", True) \
        .parquet(f"s3a://silver/erp/cat/dt={process_date}/")


    df_dim_products = df_crm_prd.alias("crm_p").join(df_erp_cat.alias("erp_ca"),\
                            col("crm_p.prd_key") == col("erp_ca.ID"), "left")\
                            .select(\
                                col("crm_p.prd_id"),\
                            row_number().over(
                                    Window.partitionBy("crm_p.prd_key").orderBy(
                                        col("crm_p.prd_start_dt"),
                                        col("crm_p.prd_key")
                                    )
                                ).alias("prd_key"),
                                col("crm_p.prd_nm"),\
                                col("erp_ca.ID"),\
                                col("erp_ca.SUBCAT"),\
                                col("erp_ca.MAINTENANCE"),\
                                col("crm_p.prd_cost"),\
                                col("crm_p.prd_line"),\
                                col("crm_p.prd_start_dt")\
                            ).filter(col("crm_p.prd_end_dt").isNull())


    return df_dim_products


def run (process_date): 
    df = gold_dim_products(process_date)
    df.show()
    output_path = f"s3a://gold/dim_products/dt={process_date}/"
    df.write.mode("overwrite").parquet(output_path)


    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/warehouse")\
        .option("dbtable", "dwh.dim_products") \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()




if __name__ == "__main__":
    process_date = 20260430
    run(process_date)
    print ('done')

