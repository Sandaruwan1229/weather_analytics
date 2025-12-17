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
                        


def parseWeatherDate(dateStr: String): java.sql.Date ={
    if (dateStr == null || dateStr.trim.isEmpty) return null
    
    val trimDate = dateStr.trim();
    val formatMD = new SimpleDateFormat("MM/dd/yyyy")
    val formatDM = new SimpleDateFormat("dd/MM/yyyy")
    
    try {
            val date = try{
              formatMD.parse(trimDate);  
            } catch {
                case _: Exception => formatDM.parse(trimDate);
            }
            new java.sql.Date(date.getTime)
        } catch {
            case _: Exception => null
        }
}

def getYear(dateStr: String): Int ={
    val date = parseWeatherDate(dateStr)
    if (date != null){
        val cal = java.util.Calendar.getInstance()
        cal.setTime(date)
        cal.get(java.util.Calendar.YEAR)
    } else 0
}

def getMonth(dateStr: String): Int ={
    val date = parseWeatherDate(dateStr)
    if (date != null){
        val cal = java.util.Calendar.getInstance()
        cal.setTime(date)
        cal.get(java.util.Calendar.MONTH) +1
    } else 0
}

def getWeekOfYear(dateStr: String): Int ={
    val date = parseWeatherDate(dateStr)
    if (date != null){
        val cal = java.util.Calendar.getInstance()
        cal.setTime(date)
        cal.get(java.util.Calendar.WEEK_OF_YEAR)
    } else 0
}

def isHighRadiation(radiation: java.lang.Double): Boolean = {
  if (radiation == null) {
    false
  } else {
    radiation > 15.0
  }
}

val parseWeatherDateUDF = udf(parseWeatherDate _)
val getYearUDF = udf(getYear _)
val getMonthUDF = udf(getMonth _)
val getWeekOfYearUDF = udf(getWeekOfYear _)
val isHighRadiationUDF = udf(isHighRadiation _)

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

val monthlyRadiationStats = joinedDF.groupBy("year_val", "month_val")
                                    .agg(
                                      sum(col("shortwave_radiation_sum")).alias("total_radiation_sum"),
                                      sum(when(col("is_high_radiation"), col("shortwave_radiation_sum")).otherwise(0)).alias("high_radiation_sum"),
                                      round(avg("shortwave_radiation_sum"),2).alias("avg_radiation")
                                    )
                                    .withColumn("high_radiation_percentage",
                                      round((col("high_radiation_sum").cast("double") / col("total_radiation_sum") * 100), 2))
                                    .withColumn("year_month", concat(col("year_val"), lit("-"), format_string("%02d", col("month_val"))))

println("Top 20 Months with highest Radiation Percentage:")
monthlyRadiationStats.orderBy(desc("high_radiation_percentage"))
                      .select("year_month", "high_radiation_percentage", "avg_radiation").show(20)


//
val monthlyStatsDF = cleanWeatherDF
  .groupBy("year_val", "month_val")
  .agg(avg("temp_max").as("avg_monthly_temp"))


val windowSpec = Window.partitionBy("year_val").orderBy(desc("avg_monthly_temp"))

val rankedMonthsDF = monthlyStatsDF
  .withColumn("rank", row_number().over(windowSpec))
  .filter($"rank" <= 1)
  .select("year_val", "month_val")

val hottestMonthsDataDF = cleanWeatherDF.join(rankedMonthsDF, Seq("year_val", "month_val"), "inner")

val resultDF = hottestMonthsDataDF
  .groupBy("year_val", "month_val", "week_val")
  .agg(max("temp_max").as("weekly_max_temp"))
  .orderBy("year_val", "month_val", "week_val")

resultDF.show(20)
