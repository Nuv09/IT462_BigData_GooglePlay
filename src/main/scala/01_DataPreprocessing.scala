import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.ml.feature.{Imputer, MinMaxScaler, OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineStage}

object GooglePlaySQLPhase1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("IT462 - Google Play Full Pipeline")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

// ============================================================
// 1 LOAD RAW DATA
// ============================================================

    println("\n================= LOAD RAW DATA =================\n")
    println("Data loading in progress...\n")

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/raw/Google-Playstore.csv")

    var workingDf = df

    println("Data loading completed successfully.\n")

// ============================================================
// 2 DATA CLEANING
// ============================================================

    println("================= DATA CLEANING =================\n")
    println("Data cleaning in progress...\n")

    println(s"Dataset Row Count BEFORE Cleaning: ${df.count()} \n")

// 1. Standardization & Type Conversion

    workingDf = workingDf
      .withColumn("Installs", regexp_replace(col("Installs"), "[+,]", "").cast(LongType))
      .withColumn(
        "Minimum Installs",
        regexp_replace(col("Minimum Installs"), ",", "").cast(LongType)
      )
      .withColumn(
        "Maximum Installs",
        regexp_replace(col("Maximum Installs"), ",", "").cast(LongType)
      )
      .withColumn("Price", regexp_replace(col("Price"), "[$]", "").cast(DoubleType))
      .withColumn("Rating", col("Rating").cast(DoubleType))
      .withColumn("Rating Count", col("Rating Count").cast(LongType))

    workingDf = workingDf.withColumn(
      "Free",
      when(col("Price") === 0, true)
        .otherwise(false)
        .cast(BooleanType)
    )

    workingDf = workingDf
      .withColumn("Released", to_date(trim(col("Released")), "MMM d, yyyy"))
      .withColumn("Last Updated", to_date(trim(col("Last Updated")), "MMM d, yyyy"))
      .withColumn("Scraped Time", to_timestamp(col("Scraped Time"), "yyyy-MM-dd HH:mm:ss"))

    workingDf = workingDf
      .withColumn(
        "Ad Supported",
        when(lower(trim(col("Ad Supported"))) === "true", true)
          .when(lower(trim(col("Ad Supported"))) === "false", false)
          .otherwise(null)
      )
      .withColumn(
        "In App Purchases",
        when(lower(trim(col("In App Purchases"))) === "true", true)
          .when(lower(trim(col("In App Purchases"))) === "false", false)
          .otherwise(null)
      )
      .withColumn(
        "Editors Choice",
        when(lower(trim(col("Editors Choice"))) === "true", true)
          .when(lower(trim(col("Editors Choice"))) === "false", false)
          .otherwise(null)
      )

// 2. Logical Error Handling

    workingDf = workingDf.filter(col("Installs") >= col("Minimum Installs"))
    workingDf = workingDf.filter(col("Rating Count") <= col("Installs"))
    workingDf = workingDf.filter(col("Last Updated") >= col("Released"))
    workingDf = workingDf.filter(col("Size") =!= "0")

    workingDf = workingDf.withColumn(
      "Size_KB",
      when(
        col("Size").contains("M"),
        regexp_replace(col("Size"), "[^0-9.]", "").cast(DoubleType) * 1024
      )
        .when(
          col("Size").contains("k"),
          regexp_replace(col("Size"), "[^0-9.]", "").cast(DoubleType)
        )
        .otherwise(null)
    )

// 3. Missing Value Handling

    val medianSize = workingDf.stat.approxQuantile("Size_KB", Array(0.5), 0.0)(0)
    workingDf = workingDf.na.fill(Map("Size_KB" -> medianSize))

    workingDf = workingDf.na.fill(
      Map(
        "Developer Website" -> "Unknown",
        "Privacy Policy" -> "Unknown",
        "Minimum Android" -> "Varies with device"
      )
    )

    workingDf = workingDf
      .withColumn(
        "Currency",
        when(col("Currency").isNull || col("Currency") === "XXX", "USD")
          .otherwise(col("Currency"))
      )

    workingDf = workingDf
      .filter(col("Developer Id").isNotNull)
      .filter(col("Developer Email").isNotNull)

// 4. Outlier Detection

    def detectOutliers(df: org.apache.spark.sql.DataFrame, columnName: String): Unit = {
      val quantiles = df.stat.approxQuantile(columnName, Array(0.25, 0.75), 0.01)
      val q1 = quantiles(0)
      val q3 = quantiles(1)
      val iqr = q3 - q1
      val lower = q1 - 1.5 * iqr
      val upper = q3 + 1.5 * iqr

      val outliers = df.filter(col(columnName) < lower || col(columnName) > upper).count()
      println(s"Outliers in $columnName: $outliers")
    }

    detectOutliers(workingDf, "Rating")
    detectOutliers(workingDf, "Rating Count")
    detectOutliers(workingDf, "Installs")
    detectOutliers(workingDf, "Price")
    detectOutliers(workingDf, "Size_KB")
    detectOutliers(workingDf, "Minimum Installs")
    detectOutliers(workingDf, "Maximum Installs")
    println()

// 5. Duplicate Check

    val duplicateCount = workingDf
      .groupBy("App Id")
      .count()
      .filter(col("count") > 1)
      .count()

    println(s"Duplicate Data: $duplicateCount \n")

// 6. Final Record Count

    println(s"Dataset Row Count AFTER Cleaning: ${workingDf.count()} \n")
    println("Data cleaning completed successfully.\n")

// ============================================================
// 3 DATA REDUCTION
// ============================================================

    println("================= DATA REDUCTION =================\n")
    println("Data reduction in progress...\n")

// 1. Before Reduction Stats

    val beforeRows = workingDf.count()
    val beforeCols = workingDf.columns.length

    println(s"BEFORE Reduction -> Rows: $beforeRows, Columns: $beforeCols")
    println("Columns BEFORE:")
    println(workingDf.columns.mkString(", "))

// 2. Data Reduction (Feature Selection)
// Goal: Predict popularity using Maximum Installs as label
// Prevent leakage: drop Installs + Minimum Installs
// Drop high-cardinality text/URL columns (no NLP)
// Keep Size_KB and drop raw Size string

    val columnsToDrop = Seq(
      "Installs", // leakage vs label (Maximum Installs)
      "Minimum Installs", // leakage vs label
      "App Name", // high-cardinality text (no NLP)
      "App Id", // identifier
      "Developer Id", // identifier (unless engineering dev features)
      "Developer Website", // URL/text noise
      "Developer Email", // noise + missing
      "Privacy Policy", // URL/text noise
      "Scraped Time", // scrape timestamp (not predictive)
      "Currency", // usually constant; verify in EDA
      "Size", // keep Size_KB instead
      "Free" // redundant from price
      // NOTE: Free is derived from Price in cleaning -> redundant.
    ).filter(workingDf.columns.contains)

    val reducedDf = workingDf.drop(columnsToDrop: _*)

// 3. After Reduction Stats

    val afterRows = reducedDf.count()
    val afterCols = reducedDf.columns.length

    println("\n")
    println(s"AFTER Reduction  -> Rows: $afterRows, Columns: $afterCols")
    println("Columns AFTER:")
    println(reducedDf.columns.mkString(", "))
    println("\n")

    println("Data reduction completed successfully.\n")

// ============================================================
// 4 DATA TRANSFORMATION
// ============================================================

    println("================= DATA TRANSFORMATION =================\n")
    println("Data transformation  in progress...\n")

// 1. Convert / Derive Data Types (Strings -> Numeric / Dates -> Features)

    var tdf = reducedDf

    tdf = tdf.withColumn(
      "min_android_version",
      regexp_extract(col("Minimum Android"), "([0-9]+\\.?[0-9]*)", 1).cast(DoubleType)
    )
    val refDate = lit("2026-03-05").cast(DateType)

    // Time-based engineered features (domain-relevant)
    tdf = tdf
      .withColumn("days_since_update", datediff(refDate, col("Last Updated")))
      .withColumn("app_age_days", datediff(refDate, col("Released")))
      .withColumn(
        "is_weekend_update",
        when(dayofweek(col("Last Updated")).isin(1, 7), 1).otherwise(0)
      )
      .withColumn(
        "ad_supported_num",
        when(col("Ad Supported") === true, 1.0)
          .when(col("Ad Supported") === false, 0.0)
          .otherwise(null)
      )
      .withColumn(
        "in_app_purchases_num",
        when(col("In App Purchases") === true, 1.0)
          .when(col("In App Purchases") === false, 0.0)
          .otherwise(null)
      )
      .withColumn(
        "editors_choice_num",
        when(col("Editors Choice") === true, 1.0)
          .when(col("Editors Choice") === false, 0.0)
          .otherwise(null)
      )

// 2. Encode Categorical Variables (Index + One-Hot) using MLlib

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

// 3. Normalize / Scale Numerical Features (Min-Max) using MLlib

    val numericCols = Array(
      "Rating",
      "Rating Count",
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

// 4. Final Feature Vector for ML / Analysis Snapshot on Full Data

    val finalAssembler = new VectorAssembler()
      .setInputCols(Array("numeric_features_scaled", "category_ohe", "content_rating_ohe"))
      .setOutputCol("features")
      .setHandleInvalid("keep")

    // Build pipeline on FULL data for analysis output (same spirit as original file)
    val pipeline = new Pipeline().setStages(
      Array[PipelineStage](
        categoryIndexer,
        contentIndexer,
        oneHot,
        numAssembler,
        scaler,
        finalAssembler
      )
    )

    val model = pipeline.fit(tdf)
    val transformed = model.transform(tdf)

// 5. Select Final Columns

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
    println("\n")

    println("Data transformation completed successfully.\n")

    println("==================================================\n")

    println("Snapshot (10 rows) AFTER Preprocessing:\n")
    finalDf.show(10, false)
    println("\n")

// ============================================================
// 5 SAVE FULL DATASET FOR RDD / SQL
// ============================================================

    println("================= SAVE FULL DATASET FOR RDD / SQL =================\n")

    finalDf.write
      .mode("overwrite")
      .parquet("data/transformed/transformed_google_play.parquet")

    println("Saved: data/transformed/transformed_google_play.parquet\n")

// ============================================================
// 6 MACHINE LEARNING PREPARATION 
// ============================================================

    println("================= MACHINE LEARNING PREPARATION =================\n")
    println("Preparing ML dataset with train/test split...\n")

    val mlBaseDf = tdf.select(
      col("Maximum Installs").cast(DoubleType).as("label"),
      col("Category"),
      col("Content Rating"),
      col("Rating").cast(DoubleType),
      col("Rating Count").cast(DoubleType),
      col("Price").cast(DoubleType),
      col("Size_KB").cast(DoubleType),
      col("min_android_version").cast(DoubleType),
      col("days_since_update").cast(DoubleType),
      col("app_age_days").cast(DoubleType),
      col("is_weekend_update").cast(DoubleType),
      col("ad_supported_num").cast(DoubleType),
      col("in_app_purchases_num").cast(DoubleType),
      col("editors_choice_num").cast(DoubleType)
    )

    val mlFilteredDf = mlBaseDf
      .filter(col("label").isNotNull)
      .filter(col("Category").isNotNull)
      .filter(col("Content Rating").isNotNull)

    val Array(trainDf, testDf) = mlFilteredDf.randomSplit(Array(0.7, 0.3), seed = 42)

    println(s"Train rows: ${trainDf.count()}")
    println(s"Test rows : ${testDf.count()}\n")

// 7. PREPROCESSING PIPELINE FOR ML (FIT ON TRAIN ONLY)

    val mlNumericCols = Array(
      "Rating",
      "Rating Count",
      "Price",
      "Size_KB",
      "min_android_version",
      "days_since_update",
      "app_age_days",
      "is_weekend_update",
      "ad_supported_num",
      "in_app_purchases_num",
      "editors_choice_num"
    )

    val imputedNumericCols = mlNumericCols.map(c => s"${c}_imp")

    val imputer = new Imputer()
      .setStrategy("median")
      .setInputCols(mlNumericCols)
      .setOutputCols(imputedNumericCols)

    val mlCategoryIndexer = new StringIndexer()
      .setInputCol("Category")
      .setOutputCol("category_index")
      .setHandleInvalid("keep")

    val mlContentIndexer = new StringIndexer()
      .setInputCol("Content Rating")
      .setOutputCol("content_rating_index")
      .setHandleInvalid("keep")

    val mlOneHot = new OneHotEncoder()
      .setInputCols(Array("category_index", "content_rating_index"))
      .setOutputCols(Array("category_ohe", "content_rating_ohe"))
      .setHandleInvalid("keep")

    val mlNumAssembler = new VectorAssembler()
      .setInputCols(imputedNumericCols)
      .setOutputCol("numeric_features")
      .setHandleInvalid("keep")

    val mlScaler = new MinMaxScaler()
      .setInputCol("numeric_features")
      .setOutputCol("numeric_features_scaled")

    val mlFinalAssembler = new VectorAssembler()
      .setInputCols(Array("numeric_features_scaled", "category_ohe", "content_rating_ohe"))
      .setOutputCol("features")
      .setHandleInvalid("keep")

    val mlPipeline = new Pipeline().setStages(
      Array[PipelineStage](
        imputer,
        mlCategoryIndexer,
        mlContentIndexer,
        mlOneHot,
        mlNumAssembler,
        mlScaler,
        mlFinalAssembler
      )
    )

    val mlModel = mlPipeline.fit(trainDf)

    val trainPrepared = mlModel.transform(trainDf).select(
      col("label"),
      col("Category"),
      col("Content Rating"),
      col("category_index"),
      col("content_rating_index"),
      col("category_ohe"),
      col("content_rating_ohe"),
      col("numeric_features"),
      col("numeric_features_scaled"),
      col("features")
    )

    val testPrepared = mlModel.transform(testDf).select(
      col("label"),
      col("Category"),
      col("Content Rating"),
      col("category_index"),
      col("content_rating_index"),
      col("category_ohe"),
      col("content_rating_ohe"),
      col("numeric_features"),
      col("numeric_features_scaled"),
      col("features")
    )

    println(s"Prepared train rows: ${trainPrepared.count()}")
    println(s"Prepared test rows : ${testPrepared.count()}\n")

    println("Train snapshot (10 rows):\n")
    trainPrepared.show(10, false)
    println("\n")

    println("Test snapshot (10 rows):\n")
    testPrepared.show(10, false)
    println("\n")

// 8. SAVE ML-READY TRAIN / TEST DATASETS

    println("================= SAVE ML DATASETS =================\n")

    trainPrepared.write
      .mode("overwrite")
      .parquet("data/ml/train_prepared_google_play.parquet")

    testPrepared.write
      .mode("overwrite")
      .parquet("data/ml/test_prepared_google_play.parquet")

    println("Saved: data/ml/train_prepared_google_play.parquet")
    println("Saved: data/ml/test_prepared_google_play.parquet\n")

    println("================= Pipeline Completed Successfully =================\n")

    spark.stop()
  }
}
