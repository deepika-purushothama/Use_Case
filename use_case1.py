from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("use_case1").master("local[*]").getOrCreate())

spark

df = spark.read.option("multiline", "true").json("/app/test.json")

df.show(truncate=False)


from pyspark.sql.functions import to_date,col
df_date = df.withColumn("timestamp",to_date(col("timestamp")))


df_date.show()


from pyspark.sql.functions import sum,desc
df_filter = df_date.where("log_level == 'ERROR'").orderBy(col('timestamp').desc(),col('server_id').desc())

df_filter.show()

from pyspark.sql.functions import to_date,lit
filter_df = df_filter.filter(to_date(col("timestamp")) >= to_date(lit("2024-12-05")))
filter_df.show()


from pyspark.sql.functions import count, desc
df_filter1 = filter_df.groupBy("server_id").agg(count('log_level')
.alias("count_of_errors")).orderBy(col("count_of_errors").desc()).limit(3)
df_filter1.show()

