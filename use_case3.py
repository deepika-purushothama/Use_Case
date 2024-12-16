from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("use_case1").master("local[*]").getOrCreate())

spark

df = spark.read.option("multiline", "true").json("/app/test.json")

df.show(truncate=False)

df = spark.read.option("multiline", "true").json("/FileStore/tables/test.json")

df.show()


df_order = df.orderBy(col("log_level"),col("message")).display()

from pyspark.sql.functions import col, count, desc
log_summary_df = df.groupBy("log_level", "message").agg(count("*").alias("message_count")).orderBy(col("log_level"))
log_summary_df.display()


from pyspark.sql.window import Window
from pyspark.sql.functions import rank

window_spec = Window.partitionBy("log_level").orderBy(desc("message_count"))
ranked_logs_df = log_summary_df.withColumn("rank", rank().over(window_spec))

ranked_logs_df.show()

top_messages_df = ranked_logs_df.filter(col("rank") == 1).select("log_level", "message", "message_count")

top_messages_df.show(truncate=False)