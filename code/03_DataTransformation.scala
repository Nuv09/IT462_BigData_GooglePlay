import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler, MinMaxScaler}
import org.apache.spark.ml.{Pipeline, PipelineStage}

object DataTransformation {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("IT462 - Google Play Data Transformation")
      .master("local[*]")
      .getOrCreate()

    // ============================================================
    // 1) Load Reduced Dataset
    // ============================================================
   
    val df = spark.read.parquet("data/reduced/reduced_google_play.parquet")

    println(s"BEFORE Transformation -> Rows: ${df.count()}, Columns: ${df.columns.length}")
    println("Columns BEFORE:")
    println(df.columns.mkString(", "))

    var tdf = df

    // ============================================================
    // 2) Convert / Derive Data Types (Strings -> Numeric / Dates -> Features)
    // ============================================================
   
    tdf = tdf.withColumn(
      "min_android_version",
      regexp_extract(col("Minimum Android"), "([0-9]+\\.?[0-9]*)", 1).cast(DoubleType)
    )

    // Time-based engineered features (domain-relevant)
    
    tdf = tdf
      .withColumn("days_since_update",
        when(col("Last Updated").isNull, lit(null).cast(IntegerType))
          .otherwise(datediff(current_date(), col("Last Updated")))
      )
      .withColumn("app_age_days",
        when(col("Released").isNull, lit(null).cast(IntegerType))
          .otherwise(datediff(current_date(), col("Released")))
      )
      // dayofweek: 1=Sunday ... 7=Saturday
      .withColumn("is_weekend_update",
        when(col("Last Updated").isNull, lit(null).cast(IntegerType))
          .otherwise(when(dayofweek(col("Last Updated")).isin(1, 7), 1).otherwise(0))
      )

    // ============================================================
    // 3) Encode Categorical Variables (Index + One-Hot) using MLlib
    // ============================================================
  
    val categoryIndexer = new StringIndexer()
      .setInputCol("Category")
      .setOutputCol("category_index")
      .setHandleInvalid("keep")

    val contentIndexer = new StringIndexer()
      .setInputCol("Content Rating")
      .setOutputCol("content_rating_index")
      .setHandleInvalid("keep")

    val oneHot = new OneHotEncoder()
      .setInputCols(Array("category_index", "content_rating_index"))
      .setOutputCols(Array("category_ohe", "content_rating_ohe"))
      .setHandleInvalid("keep")

    // ============================================================
    // 4) Normalize / Scale Numerical Features (Min-Max) using MLlib
    // ============================================================
   
    val numericCols = Array(
      "Rating",
      "Rating Count",
      "Maximum Installs",
      "Price",
      "Size_KB",
      "min_android_version",
      "days_since_update",
      "app_age_days",
      "is_weekend_update"
    )

    // Assemble numeric vector for scaling
    val numAssembler = new VectorAssembler()
      .setInputCols(numericCols)
      .setOutputCol("numeric_features")
      .setHandleInvalid("keep")

    val scaler = new MinMaxScaler()
      .setInputCol("numeric_features")
      .setOutputCol("numeric_features_scaled")

    // ============================================================
    // 5) Final Feature Vector for ML 
    // ============================================================
    
    val finalAssembler = new VectorAssembler()
      .setInputCols(Array("numeric_features_scaled", "category_ohe", "content_rating_ohe"))
      .setOutputCol("features")
      .setHandleInvalid("keep")

    // Build pipeline
    val pipeline = new Pipeline().setStages(Array[PipelineStage](
      categoryIndexer,
      contentIndexer,
      oneHot,
      numAssembler,
      scaler,
      finalAssembler
    ))

    val model = pipeline.fit(tdf)
    val transformed = model.transform(tdf)

    // ============================================================
    // 6) Select Final Columns 
    // ============================================================
  
    val finalDf = transformed.select(
      col("Category"),
      col("Content Rating"),
      col("Minimum Android"),
      col("Released"),
      col("Last Updated"),
      col("Ad Supported"),
      col("In App Purchases"),
      col("Editors Choice"),
      col("Rating"),
      col("Rating Count"),
      col("Maximum Installs"),
      col("Price"),
      col("Size_KB"),

      // Engineered / converted
      col("min_android_version"),
      col("days_since_update"),
      col("app_age_days"),
      col("is_weekend_update"),

      // Encoded
      col("category_index"),
      col("content_rating_index"),
      col("category_ohe"),
      col("content_rating_ohe"),

      // Scaled + final features vector
      col("numeric_features"),
      col("numeric_features_scaled"),
      col("features")
    )

    println(s"AFTER Transformation -> Rows: ${finalDf.count()}, Columns: ${finalDf.columns.length}")
    println("Columns AFTER:")
    println(finalDf.columns.mkString(", "))

    println("\nSnapshot (20 rows) AFTER Transformation:")
    finalDf.show(20, false)

    // ============================================================
    // 7) Save Transformed Dataset
    // ============================================================
    finalDf.write.mode("overwrite").parquet("data/transformed/transformed_google_play.parquet")
    println("Transformed dataset saved to: data/transformed/transformed_google_play.parquet")

    spark.stop()
  }
}
