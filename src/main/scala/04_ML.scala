import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.RandomForestRegressor

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
    model.write.overwrite().save("results/trained_rf_model")
    
    println("===== Model saved for evaluation ===== ")

    spark.stop()
  }
}