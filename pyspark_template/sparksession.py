from pyspark.sql import SparkSession

spark = (SparkSession.builder
            .master("local")
            .appName("pyspark_template")
            .getOrCreate())
