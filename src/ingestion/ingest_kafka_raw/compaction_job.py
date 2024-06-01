from pyspark.sql import SparkSession
import datetime

# Initialize Spark session
spark = SparkSession.builder \
    .appName("FileCompaction") \
    .getOrCreate()

# Define the path and partition structure
output_path = "path/to/output/directory"

# Function to compact files in a partition
def compact_files(year, month, day, hour):
    partition_path = f"{output_path}/year={year}/month={month}/day={day}/hour={hour}"
    df = spark.read.json(partition_path)
    df.repartition(1).write.mode("overwrite").json(partition_path)

# Example to compact files for the last hour
current_time = datetime.datetime.now()
previous_hour = current_time - datetime.timedelta(hours=1)
compact_files(previous_hour.year, previous_hour.month, previous_hour.day, previous_hour.hour)

spark.stop()
