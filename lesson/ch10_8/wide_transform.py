from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, count
import time

spark = (SparkSession.builder
         .appName("wide-transform")
         .config("spark.sql.adaptive.enabled","false")
         .config("spark.executor.cores","2")
         .config("spark.executor.memory","2g")
         .config("spark.executor.instances","3")
         .getOrCreate())

path1 = "hdfs:///home/spark/sample/linkedin_jobs/jobs/job_skills.csv"
path2 = "hdfs:///home/spark/sample/linkedin_jobs/mappings/skills.csv"

schema1= "job_id LONG, skill_abr STRING"
schema2= "skill_abr STRING, skill_name STRING"

job_df = spark.read\
                .option("header","true")\
                .option("multiLine","true")\
                .schema(schema1)\
                .csv(path1)

ski_df = spark.read\
                .option("header","true")\
                .option("multiLine","true")\
                .schema(schema2)\
                .csv(path2)

join_df = job_df.join(broadcast(ski_df), on = "skill_abr", how = "inner")

result_df = join_df\
            .groupBy(["skill_abr", "skill_name"])\
            .agg(count("job_id").alias("job_count"))\
            .orderBy("job_count", ascending = False)

result_df.show()

time.sleep(600)