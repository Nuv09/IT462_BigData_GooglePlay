import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.evaluation.RegressionEvaluator


object GooglePlaySQLPhase5 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Google Play Store - Model Training")
      .master("local[*]")
      .getOrCreate()

    // 1. Read the prepared training data
    val trainDf = spark.read.parquet("data/ml/train_prepared_google_play.parquet")

    println("===== Starting Model Training =====")

    // 2. Initialize the Random Forest Regressor 
    // We use 'label' as the target variable and 'features' as the input vector
    val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(20) 
      .setSeed(12345)  // Fixed seed for reproducibility 

    // 3. Train the model 
    val model = rf.fit(trainDf)

    println("===== Model Training Completed Successfully =====")

    // 4. Save the trained model for the Evaluation
    model.write.overwrite().save("Model/trained_rf_model")
    
    println("===== Model saved for evaluation ===== ")
  // 5. Predict on test set
    val predictions = model.transform(testDf)

    println("===== Sample Predictions =====")
    predictions.select("label", "prediction").show(10, false)

    // 6. Regression metrics
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

    val rmse = rmseEvaluator.evaluate(predictions)
    val mae  = maeEvaluator.evaluate(predictions)
    val r2   = r2Evaluator.evaluate(predictions)

    println("===== Random Forest Regression Metrics =====")
    println(s"RMSE = $rmse")
    println(s"MAE  = $mae")
    println(s"R²   = $r2")

    // 7. Baseline model: predict mean label from training set
    val meanLabel = trainDf.agg(avg("label")).first().getDouble(0)

    println(s"===== Baseline Mean Prediction = $meanLabel =====")

    val baselinePredictions = testDf.withColumn("prediction", lit(meanLabel))

    val baselineRmse = rmseEvaluator.evaluate(baselinePredictions)
    val baselineMae  = maeEvaluator.evaluate(baselinePredictions)
    val baselineR2   = r2Evaluator.evaluate(baselinePredictions)

    println("===== Baseline Regression Metrics =====")
    println(s"Baseline RMSE = $baselineRmse")
    println(s"Baseline MAE  = $baselineMae")
    println(s"Baseline R²   = $baselineR2")

    // 8. Compare model vs baseline
    println("===== Comparison =====")
    if (rmse < baselineRmse) {
      println("Model RMSE is better than baseline.")
    } else {
      println("Model RMSE is NOT better than baseline.")
    }

    if (mae < baselineMae) {
      println("Model MAE is better than baseline.")
    } else {
      println("Model MAE is NOT better than baseline.")
    }

    if (r2 > baselineR2) {
      println("Model R² is better than baseline.")
    } else {
      println("Model R² is NOT better than baseline.")
    }

    // 9. Feature importance
    println("===== Feature Importances =====")
    println(model.featureImportances)

    spark.stop()
  }
}