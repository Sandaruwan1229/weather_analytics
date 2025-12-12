import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val spark = SparkSession.builder().
  appName("Weather Analytics")
  .config("spark.sql.adaptive.enabled", "true")
  .getOrCreate()


val weatherDataPath = "/opt/resources/weather_analytics/input/weatherData.csv"
val locationDataPath = "/opt/resources/weather_analytics/input/locationData.csv"

val weatherDF = spark.read.option("header", "true")
  .option("inferSchema", "true")
  .csv(weatherDataPath)

val locationDF = spark.read.option("header", "true")
  .option("inferSchema", "true")
  .csv(locationDataPath)


val cleanWeatherDF = weatherDF
  .withColumnRenamed("shortwave_radiation_sum (MJ/m²)", "shortwave_radiation_sum")
  .filter(col("location_id").isNotNull)
  .filter(col("date").isNotNull)
  .filter(col("shortwave_radiation_sum").isNotNull)
  .withColumn("year_val", getYearUDF(col("date")))
  .withColumn("month_val", getMonthUDF(col("date")))
  .withColumn("week_val", getWeekOfYearUDF(col("date")))
  .withColumn("is_high_radiation", isHighRadiationUDF(col("shortwave_radiation_sum")))
  .withColumn("temp_max", col("temperature_2m_max (°C)").cast(DoubleType))

val joinedDF = cleanWeatherDF.join(locationDF, "location_id")


val processedDF = joinedDF
  .withColumn("precip_sum", col("precipitation_sum (mm)").cast(DoubleType))
  .withColumn("wind_gusts", col("wind_gusts_10m_max (km/h)").cast(DoubleType))
  // Assuming the 3rd temperature column in your image is the mean.
  // If not, use (max+min)/2
  .withColumn("temp_mean", col("temperature_2m_mean (°C)").cast(DoubleType))

processedDF.cache()

val seasonalPrecipDF = processedDF
  .groupBy("city_name", "month_val")
  .agg(sum("precip_sum").as("total_monthly_rain"))

// Window to rank months within each city
val windowSpec = Window.partitionBy("city_name").orderBy(desc("total_monthly_rain"))

val peakMonthsDF = seasonalPrecipDF
  .withColumn("rank", row_number().over(windowSpec))
  .filter($"rank" === 1) // Keep only the #1 wettest month
  .select("city_name", "month_val", "total_monthly_rain")
  .orderBy("city_name")

println("--- 1. Most Precipitous Month by District ---")
peakMonthsDF.show()


val topDistrictsDF = processedDF
  .groupBy("city_name")
  .agg(sum("precip_sum").as("total_historical_rain"))
  .orderBy(desc("total_historical_rain"))
  .limit(5)

println("--- 2. Top 5 Districts (Total Precipitation) ---")
topDistrictsDF.show()


// 1. Calculate average temp for every "Year-Month" combo
val monthlyTempsDF = processedDF
  .groupBy("year_val", "month_val")
  .agg(avg("temp_mean").as("avg_month_temp"))

// 2. Count Total Months vs Hot Months
val totalMonths = monthlyTempsDF.count()
val hotMonths = monthlyTempsDF.filter($"avg_month_temp" > 30.0).count()

val hotMonthPercentage = (hotMonths.toDouble / totalMonths.toDouble) * 100

println(s"--- 3. Percentage of Months > 30°C ---")
println(f"Total Months: $totalMonths")
println(f"Hot Months (>30C): $hotMonths")
println(f"Percentage: $hotMonthPercentage%.2f%%")


val extremeWeatherDF = processedDF
  .filter($"precip_sum" > 50.0 && $"wind_gusts" > 40.0)

val extremeDaysCount = extremeWeatherDF.count()

println(s"--- 4. Total Extreme Weather Days ---")
println(s"Count of days with Rain > 50mm AND Wind Gusts > 40km/h: $extremeDaysCount")


