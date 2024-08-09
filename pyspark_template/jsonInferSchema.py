from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import json

# Initialize Spark session
spark = SparkSession.builder.appName("InferSchemaFromDataset").getOrCreate()
sc = spark.sparkContext

# Example DataFrame with a column of JSON strings
data = [(1, '{"name": "Alice", "age": 25}'), (2, '{"name": "Bob", "age": 30}')]
df = spark.createDataFrame(data, ["id", "jsonString"])

# Extract the JSON column as a Dataset (in PySpark, Dataset is equivalent to DataFrame)
json_dataset = df.select("jsonString").alias("string")

# Access the JsonInferSchema object using PySpark and Scala reflection
jvm = sc._jvm

# Create an empty Scala map for options
scala_map = jvm.scala.collection.immutable.Map.empty()

# Get the session local time zone
local_time_zone = spark.sessionState().conf().sessionLocalTimeZone()

# Create JSONOptions object
json_options = jvm.org.apache.spark.sql.execution.datasources.json.JSONOptions(scala_map, local_time_zone)

# Access the JsonInferSchema class and the inferFromDataset method
json_infer_schema_class = jvm.org.apache.spark.sql.execution.datasources.json.JsonInferSchema
infer_from_dataset_method = json_infer_schema_class.getMethod(
    "inferFromDataset", jvm.org.apache.spark.sql.Dataset, jvm.org.apache.spark.sql.execution.datasources.json.JSONOptions
)

# Call the inferFromDataset method
schema = infer_from_dataset_method.invoke(None, json_dataset._jdf, json_options)

# Convert the inferred schema to a JSON string
schema_json = schema.json()

# Convert the schema JSON string to a PySpark StructType
schema_struct = StructType.fromJson(json.loads(schema_json))

# Print the inferred schema
print(schema_struct.json())
