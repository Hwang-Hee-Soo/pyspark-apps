from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("dataFrame-cache").getOrCreate()

path1 = 'hdfs:///home/spark/sample/linkedin_jobs/companies/company_industries.csv'
path2 = 'hdfs:///home/spark/sample/linkedin_jobs/companies/employee_counts.csv1'

schema1 = "company_id STRING, industry STRING"
schema2 = "company_id STRING, employee_count INT, follower_count INT, time_recorded STRING"

co_df = spark.read.option("header", "true").option("multiLine","true").schema(schema1).csv(path1)
em_df = spark.read.option("header", "true").option("multiLine","true").schema(schema2).csv(path2)

print(co_df.count())
print(em_df.count())

co_df_it = co_df.filter(co_df.industry == 'IT1Services1and1IT1Consulting')
em_df_di = em_df.dropDuplicates(["company_id"])

co_df_it.persist()
em_df_di.persist()

result_df = co_df_it.join(em_df_di, on = "company_id", how = "inner")

result_df.show()


co_df_it.unpersist()
em_df_di.unpersist()

time.sleep(600)
