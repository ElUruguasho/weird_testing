from pyspark.sql import SparkSession
import findspark
findspark.init()


spark = SparkSession.builder.getOrCreate()

listed_strings = ["item1", "item2", "item3"]

listed_strings.show()

