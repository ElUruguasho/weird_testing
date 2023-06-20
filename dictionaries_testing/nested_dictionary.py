from pyspark.sql import SparkSession
import findspark
findspark.init()


spark = SparkSession.builder.getOrCreate()

listed_strings = ["item1", "item2", "item3"]

dict_with_string = {"item1": "id1", "item2": "id2", "item3": "id3"}

dict_with_int = {"item1": 1, "item2": 2, "item3": 3}


# Test1 - With a list comprehension we convert dictionary into tuple and create a dataframe from that
data = [(key, value) for key, value in dict_with_string.items()]

dict_df_str = spark.createDataFrame(data, schema=["item", "id"])

dict_df_str.show()
dict_df_str.printSchema()


"""
-----+---+
| item| id|
+-----+---+
|item1|id1|
|item2|id2|
|item3|id3|
+-----+---+
    root
 |-- item: string (nullable = true)
 |-- id: string (nullable = true)
"""

# Test2 - With a list comprehension we convert dictionary into tuple and create a dataframe from that
data = [(key, value) for key, value in dict_with_int.items()]

dict_df_int = spark.createDataFrame(data, schema=["item", "id"])

dict_df_int.show()
dict_df_int.printSchema()

"""
-----+---+
| item| id|
+-----+---+
|item1|  1|
|item2|  2|
|item3|  3|
+-----+---+

root
 |-- item: string (nullable = true)
 |-- id: long (nullable = true)

"""

# Test3 - converting the dictionary into tuples to build a dataframe using the key with the list
nested = [listed_strings, [dict_with_string]]

# Flatten the nested list to obtain a list of tuples
flat_list = [(key, dict_with_string[key]) for key in listed_strings]

# Create a PySpark DataFrame from the list of tuples
spark = SparkSession.builder.appName("NestedListToDataFrame").getOrCreate()
nested_df = spark.createDataFrame(flat_list, ["Key", "Value"])

# Show the DataFrame
nested_df.show()
nested_df.printSchema()

"""
  Key|Value|
+-----+-----+
|item1|  id1|
|item2|  id2|
|item3|  id3|
+-----+-----+

root
 |-- Key: string (nullable = true)
 |-- Value: string (nullable = true)
"""
