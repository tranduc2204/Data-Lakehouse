import pandas
from pyspark.sql.functions import * 
from pyspark.sql.window import Window
from pyspark.sql import SparkSession


def gold_fact_table(process_date):
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


    df_dim_products = spark.read \
        .option("header", True) \
        .parquet(f"s3a://gold/dim_products/dt={process_date}/")

    df_dim_customers = spark.read \
        .option("header", True) \
        .parquet(f"s3a://gold/dim_customers/dt={process_date}/")

    df_crm_sales = spark.read \
        .option("header", True) \
        .parquet(f"s3a://silver/crm/sales/dt={process_date}/")



    df_gold_fact_table = df_crm_sales.alias("crm_s").join (df_dim_products.alias("dim_p"), col("crm_s.sls_prd_key") == col("dim_p.prd_key") , "left")\
                                .join (df_dim_customers.alias("dim_c"), col("crm_s.sls_cust_id") == col ("dim_c.cst_id"), "left")\
                                .select (\
                                    col("crm_s.sls_ord_num"),\
                                    col("dim_p.prd_key"),\
                                    col("dim_c.cst_key"),\
                                    col("crm_s.sls_order_dt"),\
                                    col("crm_s.sls_ship_dt"),\
                                    col("crm_s.sls_due_dt"),\
                                    col("crm_s.sls_sales"),\
                                    col("crm_s.sls_quantity"),\
                                    col("crm_s.sls_price")\
                                )


    return df_gold_fact_table


def run (process_date): 
    df = gold_fact_table(process_date)
    df.show()
    output_path = f"s3a://gold/fact_table/dt={process_date}/"
    df.write.mode("overwrite").parquet(output_path)

    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/warehouse")\
        .option("dbtable", "dwh.fact_table") \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()



if __name__ == "__main__":
    process_date = 20260430
    run(process_date)
    print ('done')




