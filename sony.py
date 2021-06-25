#ran the job with the following command:

#spark-submit --master yarn --deploy-mode client --files ./etl.prop sony.py etl.prop

import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, to_timestamp, col, monotonically_increasing_id, trim, rank, row_number, sum, to_date, hour, current_timestamp, lag, lit, avg
from pyspark.sql.types import IntegerType, LongType, StringType
from pyspark.sql.window import Window
import datetime
from datetime import timezone
import configparser



#creating a spark session object
spark = SparkSession.builder.appName("Rest etl").getOrCreate()

#setting the logLevel to ERROR only
spark.sparkContext.setLogLevel("ERROR")

#reading configs from the properties file that was passed
print("reading configs..")
parser = configparser.ConfigParser()

parser.read(sys.argv[1])

available_confs = []

for k,v in parser.items("conf"):
      available_confs.append(k)

now = datetime.datetime.now(timezone.utc)
curr_date = now.strftime('%Y-%m-%d')
curr_hour = now.hour


def top_restaurants_by_cuisine(from_date, until_date):
    #this method is for calculating top 3 restaurants by cuisine
    if from_date and until_date:
        df = visits.where((to_date("visit_date") <= to_date(lit(until_date))) & (to_date("visit_date") >= to_date(lit(from_date))))
    elif from_date:
        df = visits.where(to_date("visit_date") >= to_date(lit(from_date)))
    elif until_date:
        df = visits.where(to_date("visit_date") <= to_date(lit(until_date)))
    else:
        df = visits
    #performing group by place_id to get the total sales for a restaurant
    totalAmount = df.groupBy("place_id").agg(sum("sales_amount").alias("total_amount"))
    restCuisines.createOrReplaceTempView("restCuisines")
    totalAmount.createOrReplaceTempView("totalAmount")
    #performing join operation to correlate restuarants and cuisines with thier respective total_amount
    topRest = spark.sql("select served_cuisine, place_id, if(total_amount is null, 0, total_amount) as total_amount from (select served_cuisine, r.place_id as place_id, total_amount from restCuisines r left join totalAmount t on r.place_id = t.place_id)")
    windowSpec = Window.partitionBy(topRest['served_cuisine']).orderBy(topRest['total_amount'].desc())
    rnk = row_number().over(windowSpec)
    top3Rest = topRest.select("place_id","total_amount","served_cuisine",rnk.alias("rank")).where(col("rank")<=3).withColumn("from_date",lit(from_date).cast(StringType())).withColumn("until_date",lit(until_date).cast(StringType()))

    print("top 3 restaurants by cuisine:")
    top3Rest.show(20,False)
    top3Rest.printSchema()
    #saving the result with current date and hour partitioning
    top3Rest.withColumn("hour",lit(str(curr_hour))).withColumn("day",lit(curr_date)).write.format("parquet").mode("overwrite").save(parser.get("conf","spark.restetl.baselocation")+"top3_rest/day="+curr_date+"/hour="+str(curr_hour))

def nth_top_restaurant(from_date,until_date,n):
    if from_date and until_date:
        df = visits.where((to_date("visit_date") <= to_date(lit(until_date))) & (to_date("visit_date") >= to_date(lit(from_date))))
    elif from_date:
        df = visits.where(to_date("visit_date") >= to_date(lit(from_date)))
    elif until_date:
        df = visits.where(to_date("visit_date") <= to_date(lit(until_date)))
    else:
        df = visits
    totalAmount = df.groupBy("place_id").agg(sum("sales_amount").alias("total_amount"))
    restCuisines.createOrReplaceTempView("restCuisines")
    totalAmount.createOrReplaceTempView("totalAmount")
    #performing join operation to correlate restuarants and cuisines with thier respective total_amount
    topRest = spark.sql("select served_cuisine, place_id, if(total_amount is null, 0, total_amount) as total_amount from (select served_cuisine, r.place_id as place_id, total_amount from restCuisines r left join totalAmount t on r.place_id = t.place_id)")
    #creating the windowing function
    windowSpec = Window.partitionBy(topRest['served_cuisine']).orderBy(topRest['total_amount'].desc())
    rnk = row_number().over(windowSpec)
    nthTopDf = topRest.select("place_id","total_amount","served_cuisine",rnk.alias("rank")).where(col("rank")==n).withColumn("from_date",lit(from_date).cast(StringType())).withColumn("until_date",lit(until_date).cast(StringType()))
    print(str(n)+"th top restaurant for every cuisine DF:")
    nthTopDf.show(20,False)
    nthTopDf.printSchema()
    #saving the result with current date and hour partitioning
    nthTopDf.withColumn("hour",lit(str(curr_hour))).withColumn("day",lit(curr_date)).write.format("parquet").mode("overwrite").save(parser.get("conf","spark.restetl.baselocation")+"nth_top_restaurant/day="+curr_date+"/hour="+str(curr_hour))

def avg_hours_consecutive_visits(date):
    #this method is for calculating avg deifference in hours between any two consecutive restaurant visits for all users on a given date
    df = visits.where(to_date(col("visit_date")) == to_date(lit(date))) #filtering visits DF to get all the restaurant visits of all the users on the given date
    #creating the windowing function
    windowSpec = Window.partitionBy(df['user_id']).orderBy(df['visit_date'])
    prevDate = lag(df['visit_date']).over(windowSpec)
    avgHoursDf = df.select("place_id","visit_date","user_id",prevDate.alias("prev")).withColumn("DiffInSeconds",col("visit_date").cast(LongType()) - col("prev").cast(LongType())).groupBy("user_id").agg(avg("DiffInSeconds").alias("diff_secs")).withColumn("avg_diff_in_hours",col("diff_secs")/3600).drop("diff_secs")
    print("Avg diff in hours df:")
    avgHoursDf.createOrReplaceTempView("avghours")
    avgHoursDf = spark.sql("select user_id, if(avg_diff_in_hours is null, -9994, avg_diff_in_hours) as avg_diff_in_hours from avghours")
    avgHoursDf.show(20,False)
    avgHoursDf.printSchema()
    #saving the result with current date and hour partitioning
    avgHoursDf.withColumn("date_ran",lit(date)).withColumn("hour",lit(str(curr_hour))).withColumn("day",lit(curr_date)).write.format("parquet").mode("overwrite").save(parser.get("conf","spark.restetl.baselocation")+"avg_hours/day="+curr_date+"/hour="+str(curr_hour))



#Reading the raw userDetails input data
print("reading userDetails.json")
userDetails = spark.read.json(parser.get("conf","spark.restetl.baselocation")+"userDetails.json")

print("creating visits DF")
#creating a new dataframe from the 'placeInteractionDetails' column from userDetails. Adding a new column 'visit_id' to help with the removal of duplicates if present.
visits = userDetails.select(explode("placeInteractionDetails"),"userId").select(col("col.foodRating").cast(IntegerType()).alias("food_rating"),trim(col("col.placeID")).alias("place_id"),col("col.restRating").cast(IntegerType()).alias("rest_rating"),col("col.salesAmount").alias("sales_amount"),col("col.serviceRating").cast(IntegerType()).alias("service_rating"),to_timestamp("col.visitDate").alias("visit_date"),col("userId").alias("user_id")).withColumn("visit_id", monotonically_increasing_id())

#creating a temporary view on the visits dataframe
visits.createOrReplaceTempView("visits")

#removing any duplicates
visits = spark.sql("select * from visits where visit_id in (select max(visit_id) from visits group by user_id,place_id,visit_date)").drop("visit_id")

#creating a temporary view on the visits dataframe
visits.createOrReplaceTempView("visits")

#Keeping the visits DF in memory for later use.
visits.cache()

#saving visits data with current date and hour partitioning
print("saving visits DF")
visits.withColumn("hour",lit(str(curr_hour))).withColumn("day",lit(curr_date)).write.format("parquet").mode("overwrite").save(parser.get("conf","spark.restetl.baselocation")+"visits_parquet/day="+curr_date+"/hour="+str(curr_hour))

#Dropping 'placeInteractionDetails' from userDetails and renaming other columns
print("modifying userDetails DF")
userDetails = userDetails.drop("placeInteractionDetails").withColumnRenamed("hijos","children").withColumnRenamed("favCuisine", "fav_cuisine").withColumnRenamed("userPaymentMethods","user_payment_methods").withColumnRenamed("userID","user_id")

#Keeping the userDetails DF in memory for later use.
userDetails.cache()

#saving the userDetails data with current date and hour partitioning
print("saving userDetails DF")
userDetails.withColumn("hour",lit(str(curr_hour))).withColumn("day",lit(curr_date)).write.format("parquet").mode("overwrite").save(parser.get("conf","spark.restetl.baselocation")+"users_parquet/day="+curr_date+"/hour="+str(curr_hour))

#Reading raw placeDetails data
print("reading placeDetials DF")
placeDetails = spark.read.json(parser.get("conf","spark.restetl.baselocation")+"placeDetails.json")

#Renaming columns
placeDetails = placeDetails.withColumnRenamed("acceptedPayments","accepted_payments").withColumnRenamed("openHours","open_hours").withColumnRenamed("parkingType","parking_type").withColumnRenamed("placeId","place_id").withColumnRenamed("servedCuisines","served_cuisines")

#flattening the 'served_cuisines' column
print("creating restCuisnes DF")
restCuisines = placeDetails.select(explode("served_cuisines").alias("served_cuisine"),col("place_id"))

#saving the placeDetails data with current date and hour partitioning
print("Saving placeDetails DF")
placeDetails.withColumn("hour",lit(str(curr_hour))).withColumn("day",lit(curr_date)).write.format("parquet").mode("overwrite").save(parser.get("conf","spark.restetl.baselocation")+"places_parquet/day="+curr_date+"/hour="+str(curr_hour))

try:
    if parser.get("conf","spark.restetl.top3.execute").lower() == "true":
        print("Kicking off the execution of top 3 restaurants analytical function:")
        from_date, until_date = None, None
        #handling dates. if they are present and if they are in the correct format
        if "spark.restetl.top3.fromdate" in available_confs:
            datetime.datetime.strptime(parser.get("conf","spark.restetl.top3.fromdate"), '%Y-%m-%d')
            from_date = parser.get("conf","spark.restetl.top3.fromdate")
        if "spark.restetl.top3.untildate" in available_confs:
            datetime.datetime.strptime(parser.get("conf","spark.restetl.top3.untildate"), '%Y-%m-%d')
            until_date = parser.get("conf","spark.restetl.top3.untildate")
        top_restaurants_by_cuisine(from_date,until_date)
except ValueError as e:
    print(str(e)+". Not executing the top 3 restaurants analytical function")
    pass
except Exception as e:
    print(str(e)+". Not executing the top 3 restaurants analytical function")

try:
    if parser.get("conf","spark.restetl.nthtop.execute").lower() == "true":
        print("Kicking off the execution of nth top restaurant analytical function:")
        if "spark.restetl.nthtop.n" not in available_confs:
            raise Exception("n value has not been provided")
        n = int(parser.get("conf","spark.restetl.nthtop.n"))
        from_date, until_date = None, None
        #handling dates. if they are present and if they are in the correct format
        if "spark.restetl.nthtop.fromdate" in available_confs:
            datetime.datetime.strptime(parser.get("conf","spark.restetl.nthtop.fromdate"), '%Y-%m-%d')
            from_date = parser.get("conf","spark.restetl.nthtop.fromdate")
        if "spark.restetl.nthtop.untildate" in available_confs:
            datetime.datetime.strptime(parser.get("conf","spark.restetl.nthtop.untildate"), '%Y-%m-%d')
            until_date = parser.get("conf","spark.restetl.nthtop.untildate")
        nth_top_restaurant(from_date, until_date, n)
except ValueError as e:
    print(str(e)+". Not executing the nth top analytical function.")
    pass
except Exception as e:
    print(str(e)+". Not executing the nth top analytical function.")
    pass

try:
    if parser.get("conf","spark.restetl.avghours.execute") == "true":
        print("Kicking off the execution of avg hours between visits analytical function:")
        #handling date. if it is present and if it is in the correct format
        if "spark.restetl.avghours.date" not in available_confs:
            raise Exception("spark.restetl.avghours.date has not been provided")
        datetime.datetime.strptime(parser.get("conf","spark.restetl.avghours.date"), '%Y-%m-%d')
        avg_hours_consecutive_visits(parser.get("conf","spark.restetl.avghours.date"))
except ValueError as e:
    print(str(e)+". Not executing the average hours between consecutive visits analytical function.")
    pass
except Exception as e:
    print(str(e)+". Not executing the average hours between consecutive visits analytical function.")
    pass
