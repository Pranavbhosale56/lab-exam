import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum , when



spark = SparkSession.builder.appName("TitanicDataLoading").getOrCreate()


titanic_df = spark.read.csv("titanic.csv", header=True, inferSchema=True)
titanic_df.printSchema()

print("First 10 records of the Titanic DataFrame:")
titanic_df.show(10)

print("Null counts per column:")
null_counts = titanic_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in titanic_df.columns])
null_counts.show()


titanic_df_cleaned = titanic_df.dropna(subset=['age'])
print("DataFrame after removing nulls in 'age' column. New row count:")
print(titanic_df_cleaned.count())

titanic_df_with_age_group = titanic_df_cleaned.withColumn("Age_Group",
    when(col("age") < 18, "Child")
    .when((col("age") >= 18) & (col("age") < 60), "Adult")
    .otherwise("Senior")
)


print("Schema with new 'Age_Group' column:")
titanic_df_with_age_group.printSchema()

print("First 10 records with 'Age_Group' column:")
titanic_df_with_age_group.show(10)

titanic_df_with_age_group.createOrReplaceTempView("titanic")


passenger_count_per_class = spark.sql(
    """SELECT pclass, COUNT(*) 
    as passenger_count FROM
     titanic GROUP BY pclass
      ORDER BY pclass""")

print("Number of passengers in each passenger class:")
passenger_count_per_class.show()



avg_age_per_class = spark.sql(
    """SELECT pclass,
     AVG(age) as average_age
      FROM titanic
       GROUP BY pclass
        ORDER BY pclass""")

print("Average age for each passenger class:")
avg_age_per_class.show()


top_5_oldest_passengers = spark.sql(
    """SELECT age, gender, pclass, survived FROM titanic 
    ORDER BY age DESC LIMIT 5""")

print("Top 5 oldest passengers:")
top_5_oldest_passengers.show()


output_directory = "transformed_titanic_data"

titanic_df_with_age_group.write.csv(output_directory, header=True, mode="overwrite")

print(f"Transformed DataFrame saved to '{output_directory}' directory as CSV files.")