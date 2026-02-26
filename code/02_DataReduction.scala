import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataReduction {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("IT462 - Google Play Data Reduction")
      .master("local[*]")
      .getOrCreate()

    // ============================================================
    // 1) Load Cleaned Dataset (Parquet)
    // ============================================================
    val inputPath  = "data/cleaned/cleaned_google_play.parquet"
    val outputPath = "data/cleaned/reduced_google_play.parquet"

    val cleanedDf = spark.read.parquet(inputPath)

    // ============================================================
    // 2) Before Reduction Stats
    // ============================================================
    val beforeRows = cleanedDf.count()
    val beforeCols = cleanedDf.columns.length

    println(s"BEFORE Reduction -> Rows: $beforeRows, Columns: $beforeCols")
    println("Columns BEFORE:")
    println(cleanedDf.columns.mkString(", "))

    // ============================================================
    // 3) Data Reduction (Feature Selection)
    // Goal: Predict popularity using Maximum Installs as label
    // Prevent leakage: drop Installs + Minimum Installs
    // Drop high-cardinality text/URL columns (no NLP)
    // Keep Size_KB and drop raw Size string
    // ============================================================

    val columnsToDrop = Seq(
      "Installs",             // leakage vs label (Maximum Installs)
      "Minimum Installs",     // leakage vs label
      "App Name",             // high-cardinality text (no NLP)
      "App Id",               // identifier
      "Developer Id",         // identifier (unless engineering dev features)
      "Developer Website",    // URL/text noise
      "Developer Email",      // noise + missing
      "Privacy Policy",       // URL/text noise
      "Scraped Time",         // scrape timestamp (not predictive)
      "Currency",             // usually constant; verify in EDA
      "Size" ,                // keep Size_KB instead
      "Free"                  // redundant from price 
      // NOTE: Free vs Price
      // Free is derived from Price in cleaning -> redundant.
      // If your team prefers, you can also drop "Free" and keep Price only.
      // "Free"
    ).filter(cleanedDf.columns.contains)

    val reducedDf = cleanedDf.drop(columnsToDrop: _*)

    // ============================================================
    // 4) After Reduction Stats
    // ============================================================
    val afterRows = reducedDf.count()
    val afterCols = reducedDf.columns.length

    println(s"AFTER Reduction  -> Rows: $afterRows, Columns: $afterCols")
    println("Columns AFTER:")
    println(reducedDf.columns.mkString(", "))

    // ============================================================
    // 5) Snapshot (10–20 rows) for Phase 2 deliverable
    // ============================================================
    println("\nSnapshot (20 rows) AFTER Reduction:")
    reducedDf.show(20, truncate = false)

    // ============================================================
    // 6) Save Reduced Dataset
    // ============================================================
    reducedDf.write
      .mode("overwrite")
      .parquet(outputPath)

    println(s"✅ Reduced dataset saved to: $outputPath")

    spark.stop()
  }
}