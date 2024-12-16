from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("use_case1").master("local[*]").getOrCreate())

spark

df = spark.read.option("multiline", "true").json("/app/test.json")

df.show(truncate=False)

df = spark.read.option("multiline", "true").json("/FileStore/tables/test.json")

df.show()

from pyspark.sql.functions import to_date
df_date = df.withColumn("timestamp",to_date("timestamp"))
df_date.display()

from pyspark.sql.functions import to_date,col,desc,lit
df_date_filter = df_date.filter(to_date(col("timestamp")) >= to_date(lit("2024-12-05")))
df_date_filter.display()


logs_with_day = df_date_filter.withColumn("day", to_date(col("timestamp")))
logs_with_day.display()

from pyspark.sql.functions import avg,count,desc
daily_logs_df = logs_with_day.groupBy("server_id","day").agg(count("*").alias("daily_log_count")).orderBy(col("server_id"))
daily_logs_df.display()


avg_logs_df = daily_logs_df.groupBy("server_id").agg(avg("daily_log_count").alias("avg_logs_per_day")).orderBy("server_id")
avg_logs_df.display()