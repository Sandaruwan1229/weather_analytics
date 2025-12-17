import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression._
import org.apache.spark.ml.evaluation.RegressionEvaluator

val weatherDataPath = "/opt/resources/weather_analytics/input/weatherData.csv"

// Date parser (your given logic)
def parseWeatherDate(dateStr: String): java.sql.Date = {
  if (dateStr == null || dateStr.trim.isEmpty) return null
  val trimDate = dateStr.trim()
  val formatMD = new SimpleDateFormat("MM/dd/yyyy")
  val formatDM = new SimpleDateFormat("dd/MM/yyyy")
  try {
    val date = try formatMD.parse(trimDate) catch { case _: Exception => formatDM.parse(trimDate) }
    new java.sql.Date(date.getTime)
  } catch {
    case _: Exception => null
  }
}

val parseDateUDF = udf(parseWeatherDate _)

// Load CSV
val rawDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(weatherDataPath)

// Parse date + extract month/year
val weatherDF = rawDF
  .withColumn("parsed_date", parseDateUDF(col("date")))
  .withColumn("month", month(col("parsed_date")))
  .withColumn("year", year(col("parsed_date")))


// Identify All Numeric Columns Automatically
val numericCols = weatherDF.schema.fields
  .filter(f => f.dataType.simpleString == "double" || f.dataType.simpleString == "int")
  .map(_.name)

val et0Col = "et0_fao_evapotranspiration (mm)"

val corrResults = numericCols
  .filter(_ != et0Col)
  .map { colName =>
    val corr = weatherDF.stat.corr(colName, et0Col)
    (colName, corr)
  }
  .toSeq
  .toDF("Feature", "Correlation_with_ET0")
  .orderBy(desc("Correlation_with_ET0"))

corrResults.show(50, truncate = false)

val selectedDF = weatherDF
  .filter(col("month") === 5)
  .select(
    col("precipitation_hours (h)").cast("double").alias("precipitation_hours"),
    col("sunshine_duration (s)").cast("double").alias("sunshine_duration"),
    col("wind_speed_10m_max (km/h)").cast("double").alias("wind_speed"),
    col("et0_fao_evapotranspiration (mm)").cast("double").alias("et0")
  )
  .na.drop()

selectedDF.describe().show()

import org.apache.spark.ml.feature.VectorAssembler

val assembler = new VectorAssembler()
  .setInputCols(Array(
    "precipitation_hours",
    "sunshine_duration",
    "wind_speed"
  ))
  .setOutputCol("features")

val finalDF = assembler.transform(selectedDF)

import org.apache.spark.ml.evaluation.RegressionEvaluator

val evaluator = new RegressionEvaluator()
  .setLabelCol("et0")
  .setPredictionCol("prediction")

import org.apache.spark.ml.regression.LinearRegression

val lr = new LinearRegression()
  .setLabelCol("et0")
  .setFeaturesCol("features")

val lrModel = lr.fit(trainDF)
val lrPred = lrModel.transform(testDF)

val lrRMSE = evaluator.setMetricName("rmse").evaluate(lrPred)
val lrR2   = evaluator.setMetricName("r2").evaluate(lrPred)

println(s"Linear Regression -> RMSE: $lrRMSE , R2: $lrR2")

import org.apache.spark.ml.regression.RandomForestRegressor

val rf = new RandomForestRegressor()
  .setLabelCol("et0")
  .setFeaturesCol("features")
  .setNumTrees(50)

val rfModel = rf.fit(trainDF)
val rfPred = rfModel.transform(testDF)

val rfRMSE = evaluator.setMetricName("rmse").evaluate(rfPred)
val rfR2   = evaluator.setMetricName("r2").evaluate(rfPred)

println(s"Random Forest -> RMSE: $rfRMSE , R2: $rfR2")

import org.apache.spark.ml.regression.GBTRegressor

val gbt = new GBTRegressor()
  .setLabelCol("et0")
  .setFeaturesCol("features")
  .setMaxIter(100)   // boosting rounds
  .setMaxDepth(5)    // tree depth

val gbtModel = gbt.fit(trainDF)
val gbtPred = gbtModel.transform(testDF)

val gbtRMSE = evaluator.setMetricName("rmse").evaluate(gbtPred)
val gbtR2   = evaluator.setMetricName("r2").evaluate(gbtPred)

println(s"Gradient Boosted Trees -> RMSE: $gbtRMSE , R2: $gbtR2")

val comparisonDF = Seq(
  ("Linear Regression", lrRMSE, lrR2),
  ("Random Forest", rfRMSE, rfR2),
  ("Gradient Boosted Trees", gbtRMSE, gbtR2)
).toDF("Model", "RMSE", "R2")

comparisonDF.show()

// Filter the predictions to find days where the model predicts Low ET0 (< 1.5)
val lowET0Conditions = gbtPred
  .filter($"prediction" < 1.5)

println("Analyzing weather conditions required for Low ET0 (< 1.5mm)...")

// Calculate the average weather for these specific low-evaporation days
val requiredConditions = lowET0Conditions
  .agg(
    round(avg("precipitation_hours"), 2).alias("avg_required_precip"),
    round(avg("sunshine_duration"), 2).alias("avg_required_sunshine"),
    round(avg("wind_speed"), 2).alias("avg_required_wind"),
    round(avg("prediction"), 2).alias("avg_predicted_et0")
  )

requiredConditions.show()