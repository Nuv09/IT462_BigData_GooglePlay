import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler, MinMaxScaler}
import org.apache.spark.ml.{Pipeline, PipelineStage}

object GooglePlayFullPipeline {

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
    }

    detectOutliers(workingDf, "Rating")
    detectOutliers(workingDf, "Rating Count")
    detectOutliers(workingDf, "Installs")
    detectOutliers(workingDf, "Price")
    detectOutliers(workingDf, "Size_KB")
    detectOutliers(workingDf, "Minimum Installs")
    detectOutliers(workingDf, "Maximum Installs")

// 5. Duplicate Check

    val duplicateCount = workingDf
      .groupBy("App Id")
      .count()
      .filter(col("count") > 1)
      .count()

    println(s"Duplicate Data: $duplicateCount \n")

// 6. Final Record Count

    println(s"Final Cleaned Dataset Count: ${workingDf.count()} \n")
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
      ) // dayofweek: 1=Sunday ... 7=Saturday

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

// 4. Final Feature Vector for ML

    val finalAssembler = new VectorAssembler()
      .setInputCols(Array("numeric_features_scaled", "category_ohe", "content_rating_ohe"))
      .setOutputCol("features")
      .setHandleInvalid("keep")

    // Build pipeline
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

    println("================= Pipeline Completed Successfully =================\n")

    spark.stop()
  }
}
