import time, sys
from pyspark.sql.functions import col, lit
from pyspark.sql import SparkSession


from pyspark.sql.functions import month, desc

def max_tip_trip_in_march_to_battery_park(taxi_trips, taxi_zone_lookup):
    # Filter trips made in March
    march_trips = taxi_trips.filter(month(col("tpep_pickup_datetime")) == 3)

    # Join with the zone lookup table on pickup location and zone name
    battery_park_trips = march_trips.join(taxi_zone_lookup, [march_trips.DOLocationID == taxi_zone_lookup._c0, taxi_zone_lookup._c2 == "Battery Park"])

    # Sort trips by tip amount in descending order
    sorted_trips = battery_park_trips.sort(desc("tip_amount"))

    # Drop unnecessary columns and return the first row (trip with highest tip amount)
    return sorted_trips.drop("_c0","_c1","_c2","_c3").first()

from pyspark.sql.functions import month, asc, max

def highest_toll_per_month(taxi_trips):
    # Filter trips with non-zero tolls
    toll_trips = taxi_trips.filter(col("Tolls_amount") > 0)

    # Group by pickup month and find the maximum toll amount
    monthly_max_tolls = toll_trips.groupBy(month(col("tpep_pickup_datetime")))\
                            .agg(max("Tolls_amount").alias("max_tolls"))

    # Join with original data to get trip details for trips with highest tolls
    highest_tolls = monthly_max_tolls.join(toll_trips, [month(col("tpep_pickup_datetime")) == col("month(tpep_pickup_datetime)"), col("Tolls_amount") == col("max_tolls")])\
                            .drop("month(tpep_pickup_datetime)", "max_tolls")

    # Return the results as a collection
    return highest_tolls.collect()

from pyspark.sql.functions import col, sum, desc, row_number, asc, max, month, dayofmonth
from pyspark.sql.window import Window

def top_five_days_by_tip_percentage(df_taxi_trips):
    """
    Find the top 5 days in each month where the percentage of tips was the highest.
    :param df_taxi_trips: The dataframe containing information about taxi trips.
    :return: The result as an array of rows.
    """
    # Calculate the sum of fare amount and tip amount for each day of each month
    sum_by_month_day = df_taxi_trips.groupBy([month(col("tpep_pickup_datetime")), dayofmonth(col("tpep_pickup_datetime"))])\
        .agg(sum("Fare_amount").alias("sum_fare_amount"), sum("Tip_amount").alias("sum_tip_amount"))

    # Calculate the tip percentage for each day of each month
    with_tip_percentage = sum_by_month_day.withColumn("tip_percentage", col("sum_tip_amount")/col("sum_fare_amount"))

    # Add a column to rank the days by tip percentage in each month
    with_index = with_tip_percentage.withColumn("index", row_number().over(Window.partitionBy("month(tpep_pickup_datetime)").orderBy(desc("tip_percentage"))))

    # Filter to only include the top 5 days by tip percentage for each month
    top_five = with_index.filter(col("index") <= 5)\
        .sort(asc("month(tpep_pickup_datetime)"), asc("index"))

    # Drop the intermediate columns (sum_fare_amount, sum_tip_amount) and return the result
    return top_five.drop("sum_fare_amount", "sum_tip_amount").collect()

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, desc, row_number, asc, max, hour,dayofweek
from pyspark.sql.window import Window

def find_top3_busiest_hour_of_week(taxi_trip_data):
    """
    Find the top three busiest hours of the day for each day of the week in all months. 
    The busiest hour is defined as the hour with the highest average number of passengers in a taxi trip.
    """
    # Group data by day of week and hour
    grouped_data = taxi_trip_data.groupBy([dayofweek(col("tpep_pickup_datetime")), hour(col("tpep_pickup_datetime"))])\
    .agg(avg("passenger_count").alias("avg_passenger_count"))

    # Add a row number column to the grouped data, partitioned by the day of week and ordered by the avg passenger count
    with_row_number = grouped_data.withColumn("row_number", row_number().over(Window.partitionBy("dayofweek(tpep_pickup_datetime)").orderBy(desc("avg_passenger_count"))))

    # Filter the data to include only the top 3 busiest hours of the day
    top3_hours = with_row_number.filter(col("row_number") <= 3)\
                                 .sort(asc("dayofweek(tpep_pickup_datetime)"),asc("row_number"))

    # Return the result
    return top3_hours.collect()
# Import the necessary functions
from pyspark.sql.functions import col, avg, row_number, asc, month, dayofmonth, round, floor
from pyspark.sql.window import Window

# Define a function to calculate the average distance and cost per 15 days
def compute_avg_distance_cost(df_taxi_trips):
    # Filter out the rows where the pickup and dropoff locations are the same
    df_filtered = df_taxi_trips.filter(col("PULocationID") != col("DOLocationID"))

    # Group the data by day and month of pickup and calculate average distance and cost
    df_grouped = df_filtered.groupBy([dayofmonth(col("tpep_pickup_datetime")), month(col("tpep_pickup_datetime"))])\
        .agg(avg("trip_distance").alias("avg_trip_distance"), avg("total_amount").alias("avg_total_amount"))

    # Sort the data by month and day of pickup
    df_sorted = df_grouped.sort(asc("month(tpep_pickup_datetime)"), asc("dayofmonth(tpep_pickup_datetime)"))

    # Add an index column for each row
    df_indexed = df_sorted.withColumn("index", row_number().over(Window.orderBy("month(tpep_pickup_datetime)", "dayofmonth(tpep_pickup_datetime)")))

    # Create a new column "group" that groups the data in sets of 15 days
    df_grouped_again = df_indexed.withColumn("group", floor((col("index") - 1) / 15))

    # Group the data by the newly created "group" column and calculate the average distance and cost
    df_final = df_grouped_again.groupBy("group")\
        .agg(round(avg("avg_trip_distance"), 2).alias("15_day_avg_trip_distance"), round(avg("avg_total_amount"), 2).alias("15_day_avg_total_amount"))

    # Collect the results as an array of rows
    result = df_final.collect()

    return result



# user input
theQuery = input("Select query (1-5): ")
theQuery = int(theQuery)
typeQuery = "10"

if not (1 <= theQuery <= 5):
    print("Wrong query number. Exiting...")
    sys.exit()


# initiate Spark session
spark = SparkSession.builder.master("spark://192.168.0.2:7077").appName("advancedDB").getOrCreate()
print("Spark session built")
dataPath = "hdfs://192.168.0.2:9000/user/user/data/"

# read taxi trip data from Parquet files
DfTaxiTrips = spark.read.parquet(
    dataPath + "yellow_tripdata_2022-01.parquet",
    dataPath + "yellow_tripdata_2022-02.parquet",
    dataPath + "yellow_tripdata_2022-03.parquet",
    dataPath + "yellow_tripdata_2022-04.parquet",
    dataPath + "yellow_tripdata_2022-05.parquet",
    dataPath + "yellow_tripdata_2022-06.parquet"
)


# filter out trips from the first 6 months of 2022
DfTaxiTrips = DfTaxiTrips.filter(
    (col("tpep_pickup_datetime") >= lit("2022-01-01")) & 
    (col("tpep_pickup_datetime") < lit("2022-07-01"))
)


# convert the filtered taxi trip data to an RDD
RDDTaxiTrips = DfTaxiTrips.rdd

# read taxi zone data from a CSV file
DFTaxiZoneLookup = spark.read.csv(dataPath + "taxi_zone_lookup.csv")

# convert the taxi zone dataframe to an RDD
RDDTaxiZoneLookup = DFTaxiZoneLookup.rdd

# Start timer for the query
startTime = time.time()



# Q1
if theQuery == 1:
    max_tip_trip_in_march_to_battery_park_results = max_tip_trip_in_march_to_battery_park(DfTaxiTrips, DFTaxiZoneLookup)
    endQ1 = time.time()
    print(max_tip_trip_in_march_to_battery_park_results)
    print(f'Q1 took: {endQ1-startTime} seconds.')

# Q2
if theQuery == 2:
    highest_toll_per_month_results = highest_toll_per_month(DfTaxiTrips)
    endQ2 = time.time()
    for result in highest_toll_per_month_results:
        print(result)
    print(f'Q2 took: {endQ2-startTime} seconds.')

# Q3
if theQuery == 3:

    compute_avg_distance_cost_results = compute_avg_distance_cost(DfTaxiTrips)
    endQ3_DF = time.time()
    for res in compute_avg_distance_cost_results:
        print(res)
    print(f'Q3_DF took: {endQ3_DF-startTime} seconds.')
       

# Q4
if theQuery == 4:
    find_top3_busiest_hour_of_week_results =  find_top3_busiest_hour_of_week(DfTaxiTrips)
    endQ4 = time.time()
    for res in find_top3_busiest_hour_of_week_results:
        print(res)
    print(f'Q4 took: {endQ4-startTime} seconds.')

# Q5
if theQuery == 5:
    top_five_days_by_tip_percentage_results = top_five_days_by_tip_percentage(DfTaxiTrips)
    endQ5 = time.time()
    for res in top_five_days_by_tip_percentage_results:
        print(res)
    print(f'Q5 took: {endQ5-startTime} seconds.')













