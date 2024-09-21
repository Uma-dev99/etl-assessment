from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, sum, last
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("EnrollmentETL").getOrCreate()

# Load the Both csv files
school_df = spark.read.csv("BrazilEduPanel_School.csv", header=True, inferSchema=True)
municipal_df = spark.read.csv("BrazilEduPanel_Municipal.csv", header=True, inferSchema=True)

# Join the datasets on Municipality Code and State Code
joined_df = school_df.join(municipal_df, on=["MUNICIPALITY_CODE", "STATE_CODE"], how="inner")

# Handling missing enrolment dates: Imputing with the most recent non-null date
joined_df = joined_df.withColumn("enrolment_date", last(col("enrolment_date"), True).over(Window.orderBy("enrolment_date")))

# Remove duplicate enrolments based on ENTITY_CODE and enrolment_date
joined_df = joined_df.dropDuplicates(["CO_ENTIDAE", "CO_MUNICIPIO"])

joined_df = joined_df.dropDuplicates(["UF", "NU_ANO_CENSO"])
# Filter data for the year 2021 and group by state for total enrolments by state
df_2021 = joined_df.filter(year("enrolment_date") == 2021)
enrolments_by_state = df_2021.groupBy("state_name").agg(sum("enrolment_count").alias("total_enrolments"))

# Group by city and sum enrolments, then get the top 10 cities by enrolments
enrolments_by_city = df_2021.groupBy("city_name").agg(sum("enrolment_count").alias("total_enrolments"))
top_10_cities = enrolments_by_city.orderBy(col("total_enrolments").desc()).limit(10)

# Filter for the 10-year period (2012-2021) and group by year, state, and city for enrolments by year
df_10_years = joined_df.filter(year("enrolment_date").between(2012, 2021))
enrolments_by_year = df_10_years.groupBy(year("enrolment_date").alias("year"), "state_name", "city_name").agg(sum("enrolment_count").alias("total_enrolments"))

# Save the processed DataFrame to PostgreSQL database on localhost
joined_df.write \
  .format("jdbc") \
  .option("url", "jdbc:postgresql://localhost:5432/enrolments_db") \
  .option("dbtable", "enrolments_db") \
  .option("user", "your_postgres_user") \
  .option("password", "your_postgres_password") \
  .save()