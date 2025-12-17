import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat

val spark = SparkSession.builder().appName("MayET0Analysis").master("local[*]").getOrCreate()
import spark.implicits._
val weatherDataPath = "/opt/resources/weather_analytics/input/weatherData.csv"
val locationDataPath = "/opt/resources/weather_analytics/input/locationData.csv"

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


// 1. Load Data
val rawDF = spark.read.option("header", "true").option("inferSchema", "true").csv(weatherDataPath)
def getMonth(dateStr: String): Int ={
  val date = parseWeatherDate(dateStr)
  if (date != null){
    val cal = java.util.Calendar.getInstance()
    cal.setTime(date)
    cal.get(java.util.Calendar.MONTH) +1
  } else 0
}

val getMonthUDF = udf(getMonth _)

// 2. Data Cleaning & Filtering for MAY
// We perform this analysis specifically on May data to learn that month's specific climate patterns.
val mayDataDF = rawDF
  .filter(col("date").isNotNull)
  .withColumn("month_val",  getMonthUDF(col("date")))
  // Ensure numeric types for features
  .withColumn("precip", col("precipitation_hours (h)").cast("double"))
  .withColumn("sunshine", col("sunshine_duration (s)").cast("double"))
  .withColumn("wind", col("wind_speed_10m_max (km/h)").cast("double"))
  .withColumn("et0", col("et0_fao_evapotranspiration (mm)").cast("double"))
  .filter($"month_val" === 5) // Filter ONLY for May
  .na.drop() // Remove rows with nulls to prevent ML crashes

// 3. Feature Selection: Assemble features into a single vector column
val assembler = new VectorAssembler()
  .setInputCols(Array("precip", "sunshine", "wind"))
  .setOutputCol("features")

val finalData = assembler.transform(mayDataDF).select("features", "et0", "precip", "sunshine", "wind")

val Array(trainingData, validationData) = finalData.randomSplit(Array(0.8, 0.2), seed = 1234L)

println(s"Training Records: ${trainingData.count()}")
println(s"Validation Records: ${validationData.count()}")

// Initialize and Train Model
val lr = new LinearRegression()
  .setLabelCol("et0")
  .setFeaturesCol("features")

val lrModel = lr.fit(trainingData)

// Print coefficients to understand the relationship (Optional but helpful)
println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
// This tells you mathematically how much wind/sun increases ET0

val predictions = lrModel.transform(validationData)

val evaluator = new RegressionEvaluator()
  .setLabelCol("et0")
  .setPredictionCol("prediction")
  .setMetricName("rmse") // Root Mean Squared Error

val rmse = evaluator.evaluate(predictions)
println(s"Root Mean Squared Error (RMSE): $rmse")
// If RMSE is low (e.g., < 0.5), the model is good at predicting ET0.

// Filter the predictions to find days where the model predicts Low ET0 (< 1.5)
val lowET0Conditions = predictions
  .filter($"prediction" < 1.5)

println("Analyzing weather conditions required for Low ET0 (< 1.5mm)...")

// Calculate the average weather for these specific low-evaporation days
val requiredConditions = lowET0Conditions
  .agg(
    round(avg("precip"),2).alias("avg_required_precip"),
    round(avg("sunshine"), 2).alias("avg_required_sunshine"),
    round(avg("wind"), 2).alias("avg_required_wind"),
    round(avg("prediction"), 2).alias("avg_predicted_et0")
  )

requiredConditions.show()