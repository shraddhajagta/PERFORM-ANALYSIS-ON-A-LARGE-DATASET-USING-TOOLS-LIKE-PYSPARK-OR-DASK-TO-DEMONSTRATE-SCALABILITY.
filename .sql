from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, desc
spark = SparkSession.builder \
    .appName("Customer Transaction Analysis") \
    .getOrCreate()
df = spark.read.csv("transactions.csv", header=True, inferSchema=True)
df.printSchema()  
df.show(5)       
customer_transactions = df.groupBy("customer_id") \
    .agg(count("transaction_id").alias("total_transactions")) \
    .orderBy(desc("total_transactions"))
customer_spending = df.groupBy("customer_id") \
    .agg(sum("amount").alias("total_spent")) \
    .orderBy(desc("total_spent"))
category_avg = df.groupBy("category") \
    .agg(avg("amount").alias("avg_amount")) \
    .orderBy(desc("avg_amount"))
customer_transactions.show(10)
customer_spending.show(10)
category_avg.show(10)
customer_spending.write.csv("customer_spending_output.csv", header=True)
