import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DataPreprocessing {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("IT462 - Google Play Data Preprocessing")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // ============================================================
    // 1. Load Raw Dataset
    // ============================================================

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/raw/Google-Playstore.csv")

    var cleanedDf = df

    println("Initial Schema:")
    cleanedDf.printSchema()

// ============================================================
// 2. Standardization & Type Conversion
// ============================================================

// helper: keep only digits; if empty -> null; then cast to Long
def digitsOrNull(c: String) = {
  val cleaned = regexp_replace(col(c), "[^0-9]", "")
  when(length(cleaned) === 0, lit(null)).otherwise(cleaned).cast(LongType)
}

cleanedDf = cleanedDf
  .withColumn("Installs", digitsOrNull("Installs"))
  .withColumn("Minimum Installs", digitsOrNull("Minimum Installs"))
  .withColumn("Maximum Installs", digitsOrNull("Maximum Installs"))
  .withColumn("Price", regexp_replace(col("Price"), "[$]", "").cast(DoubleType))
  .withColumn("Rating", col("Rating").cast(DoubleType))
  .withColumn("Rating Count", col("Rating Count").cast(LongType))

    // Derive Free column based on price
    cleanedDf = cleanedDf.withColumn(
      "Free",
      when(col("Price") === 0, true).otherwise(false).cast(BooleanType)
    )

    // Convert date columns
    cleanedDf = cleanedDf
      .withColumn("Released", to_date(trim(col("Released")), "MMM d, yyyy"))
      .withColumn("Last Updated", to_date(trim(col("Last Updated")), "MMM d, yyyy"))
      .withColumn("Scraped Time", to_timestamp(col("Scraped Time"), "yyyy-MM-dd HH:mm:ss"))

    // Normalize Boolean fields
    cleanedDf = cleanedDf
      .withColumn("Ad Supported",
        when(lower(trim(col("Ad Supported"))) === "true", true)
          .when(lower(trim(col("Ad Supported"))) === "false", false)
          .otherwise(null))
      .withColumn("In App Purchases",
        when(lower(trim(col("In App Purchases"))) === "true", true)
          .when(lower(trim(col("In App Purchases"))) === "false", false)
          .otherwise(null))
      .withColumn("Editors Choice",
        when(lower(trim(col("Editors Choice"))) === "true", true)
          .when(lower(trim(col("Editors Choice"))) === "false", false)
          .otherwise(null))

    println("Schema After Standardization:")
    cleanedDf.printSchema()

    // ============================================================
    // 3. Logical Error Handling
    // ============================================================

    cleanedDf = cleanedDf.filter(col("Installs") >= col("Minimum Installs"))
    cleanedDf = cleanedDf.filter(col("Rating Count") <= col("Installs"))
    cleanedDf = cleanedDf.filter(col("Last Updated") >= col("Released"))
    cleanedDf = cleanedDf.filter(col("Size") =!= "0")

    // Convert Size to KB
    cleanedDf = cleanedDf.withColumn(
      "Size_KB",
      when(col("Size").contains("M"),
        regexp_replace(col("Size"), "[^0-9.]", "").cast(DoubleType) * 1024)
        .when(col("Size").contains("k"),
          regexp_replace(col("Size"), "[^0-9.]", "").cast(DoubleType))
        .otherwise(null)
    )

    // ============================================================
    // 4. Missing Value Handling
    // ============================================================

    // Encode missing categorical attributes
    cleanedDf = cleanedDf.na.fill(Map(
      "Developer Website" -> "Unknown",
      "Privacy Policy" -> "Unknown",
      "Minimum Android" -> "Varies with device"
    ))

    // Impute numeric missing values (median for Size_KB)
    val medianSize = cleanedDf.stat.approxQuantile("Size_KB", Array(0.5), 0.0)(0)
    cleanedDf = cleanedDf.na.fill(Map("Size_KB" -> medianSize))

    // Replace invalid currency codes
    cleanedDf = cleanedDf.withColumn("Currency",
      when(col("Currency").isNull || col("Currency") === "XXX", "USD")
        .otherwise(col("Currency"))
    )

    // Drop rows with critical missing identifiers
    cleanedDf = cleanedDf
      .filter(col("Developer Id").isNotNull)
      .filter(col("Developer Email").isNotNull)

    // ============================================================
    // 5. Duplicate Check
    // ============================================================

    val duplicateCount = cleanedDf.groupBy("App Id")
      .count()
      .filter(col("count") > 1)
      .count()

    println(s"Duplicate App IDs: $duplicateCount")

    // ============================================================
    // 6. Outlier Detection (IQR Method - Detection Only)
    // ============================================================

    def detectOutliers(columnName: String): Unit = {
      val quantiles = cleanedDf.stat.approxQuantile(columnName, Array(0.25, 0.75), 0.01)
      val q1 = quantiles(0)
      val q3 = quantiles(1)
      val iqr = q3 - q1
      val lower = q1 - 1.5 * iqr
      val upper = q3 + 1.5 * iqr

      val outliers = cleanedDf.filter(col(columnName) < lower || col(columnName) > upper).count()
      println(s"$columnName Outliers Count: $outliers")
    }

    detectOutliers("Rating")
    detectOutliers("Rating Count")
    detectOutliers("Installs")
    detectOutliers("Price")
    detectOutliers("Size_KB")

    // ============================================================
    // 7. Final Record Count
    // ============================================================

    println(s"Final Cleaned Dataset Count: ${cleanedDf.count()}")
    
// ============================================================
// 8. Save Cleaned Dataset
// ============================================================

cleanedDf
  .write
  .mode("overwrite")
  .parquet("data/cleaned/cleaned_google_play.parquet")

println("Cleaned dataset saved to data/cleaned/")

    spark.stop()
  }
}
