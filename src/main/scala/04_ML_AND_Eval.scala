import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.evaluation.RegressionEvaluator

object GooglePlaySQLPhase5 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Google Play Store - Model Training and Evaluation")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    println("===== Starting Model Training =====")

// 1. Read the prepared training and test data
    val trainDf = spark.read.parquet("data/ml/train_prepared_google_play.parquet")
    val testDf = spark.read.parquet("data/ml/test_prepared_google_play.parquet")

// 2. Initialize the Random Forest Regressor
    val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(20)
      .setSeed(12345)

// 3. Train the model
    val model = rf.fit(trainDf)

    println("===== Model Training Completed Successfully =====")

// 4. Save the trained model
    model.write.overwrite().save("Model/trained_rf_model")

    println("===== Model saved for evaluation =====")

// 5. Predict on training and test sets
    val trainPredictions = model.transform(trainDf)
    val testPredictions = model.transform(testDf)

    println("===== Sample Training Predictions =====")
    trainPredictions.select("label", "prediction").show(10, false)

    println("===== Sample Test Predictions =====")
    testPredictions.select("label", "prediction").show(10, false)

// 6. Regression evaluators
    val rmseEvaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val maeEvaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("mae")

    val r2Evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("r2")

// Training metrics
    val trainRmse = rmseEvaluator.evaluate(trainPredictions)
    val trainMae = maeEvaluator.evaluate(trainPredictions)
    val trainR2 = r2Evaluator.evaluate(trainPredictions)

    println("===== Random Forest Training Metrics =====")
    println(s"Training RMSE = $trainRmse")
    println(s"Training MAE  = $trainMae")
    println(s"Training R²   = $trainR2")

// Test metrics
    val testRmse = rmseEvaluator.evaluate(testPredictions)
    val testMae = maeEvaluator.evaluate(testPredictions)
    val testR2 = r2Evaluator.evaluate(testPredictions)

    println("===== Random Forest Test Metrics =====")
    println(s"Test RMSE = $testRmse")
    println(s"Test MAE  = $testMae")
    println(s"Test R²   = $testR2")

// 7. Baseline model: predict mean label from training set
    val meanLabel = trainDf.agg(avg("label")).first().getDouble(0)

    println(s"===== Baseline Mean Prediction = $meanLabel =====")

    val baselinePredictions = testDf.withColumn("prediction", lit(meanLabel))

    val baselineRmse = rmseEvaluator.evaluate(baselinePredictions)
    val baselineMae = maeEvaluator.evaluate(baselinePredictions)
    val baselineR2 = r2Evaluator.evaluate(baselinePredictions)

    println("===== Baseline Regression Metrics =====")
    println(s"Baseline RMSE = $baselineRmse")
    println(s"Baseline MAE  = $baselineMae")
    println(s"Baseline R²   = $baselineR2")

// 8. Compare model vs baseline using TEST metrics
    println("===== Comparison =====")
    if (testRmse < baselineRmse) {
      println("Model RMSE is better than baseline.")
    } else {
      println("Model RMSE is NOT better than baseline.")
    }

    if (testMae < baselineMae) {
      println("Model MAE is better than baseline.")
    } else {
      println("Model MAE is NOT better than baseline.")
    }

    if (testR2 > baselineR2) {
      println("Model R² is better than baseline.")
    } else {
      println("Model R² is NOT better than baseline.")
    }

// 9. Feature importance
    println("===== Feature Importances =====")
    println(model.featureImportances)

    import java.awt.{Color, Font, Graphics2D}
import java.awt.image.BufferedImage
import javax.imageio.ImageIO
import java.io.File

// Top 10 feature importances
val topFeatures = Seq(
  ("Category Feature", 0.2156),
  ("Rating Count", 0.1241),
  ("Editors Choice", 0.1058),
  ("Minimum Android Version", 0.1002),
  ("Rating", 0.0797),
  ("App Age Days", 0.0623),
  ("Days Since Update", 0.0619),
  ("App Size KB", 0.0586),
  ("Category Feature 2", 0.0576),
  ("Category Feature 3", 0.0439)
)

// Create image
val width = 1000
val height = 650
val image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
val g = image.createGraphics()

// Background
g.setColor(Color.WHITE)
g.fillRect(0, 0, width, height)

// Title
g.setColor(Color.BLACK)
g.setFont(new Font("Arial", Font.BOLD, 24))
g.drawString("Top 10 Feature Importances", 330, 40)

// Chart settings
val leftMargin = 260
val topMargin = 80
val barHeight = 35
val gap = 18
val maxBarWidth = 600
val maxImportance = topFeatures.map(_._2).max

g.setFont(new Font("Arial", Font.PLAIN, 16))

topFeatures.zipWithIndex.foreach { case ((feature, importance), i) =>
  val y = topMargin + i * (barHeight + gap)
  val barWidth = ((importance / maxImportance) * maxBarWidth).toInt

  // Feature name
  g.setColor(Color.BLACK)
  g.drawString(feature, 30, y + 24)

  // Bar
  g.setColor(new Color(79, 129, 189))
  g.fillRect(leftMargin, y, barWidth, barHeight)

  // Value
  g.setColor(Color.BLACK)
  g.drawString(f"$importance%.4f", leftMargin + barWidth + 10, y + 24)
}

g.dispose()

// Save image
new File("results").mkdirs()
ImageIO.write(image, "png", new File("top_feature_importances.png"))

println("Bar chart saved to: top_feature_importances.png")

    import org.apache.spark.ml.linalg.Vector

    println("Category OHE size:")
    println(trainDf.select("category_ohe").head().getAs[Vector]("category_ohe").size)

    println("Content Rating OHE size:")
    println(trainDf.select("content_rating_ohe").head().getAs[Vector]("content_rating_ohe").size)

    spark.stop()
  }
}
