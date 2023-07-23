from pyspark.sql.functions import split, regexp_extract, count, sum, when
from pyspark.sql.types import StringType, StructField, StructType

# Define the schema
schema = StructType([
  StructField("line", StringType(), True)
])

# Create a DataFrame from the data with the defined schema
df = spark.read.schema(schema).text("/test_data.txt")

# Split line by whitespace into columns and map to specific columns
df = df.withColumn('Request', split(regexp_extract(df['line'], r'"([^"]*)"', 1), '\s+'))
df = df.withColumn('LineParts', split(df['line'], '\s+'))
df = df.select(
  df['LineParts'][0].alias('IP'),
  df['Request'][0].alias('Method'),
  df['Request'][1].alias('Request'),
  df['LineParts'][8].alias('StatusCode'),
  df['LineParts'][10].alias('Bytes')
)
df = df.withColumn("Bytes", when(df["Bytes"] == "-", 0).otherwise(df["Bytes"]))

# IP count
ip_counts = df.groupBy('IP').agg(count('*').alias('count'))
ip_counts.show(truncate=False)

# Group by IP, Request, Method, StatusCode and calculate the sum of bytes and count
result = df.groupBy('IP', 'Request', 'Method', 'StatusCode').agg(sum('Bytes').alias('TotalBytes'), count('*').alias('Count'))
result.show(n=100)