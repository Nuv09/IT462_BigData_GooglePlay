import org.apache.spark.sql.SparkSession

object GooglePlayRDDPhase3 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("IT462 - Google Play Phase 3 RDD")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // ============================================================
    // Read preprocessed parquet dataset from Phase 2
    // ============================================================
    val finalDf = spark.read.parquet("data/transformed/transformed_google_play.parquet")

    println("\n================= PHASE 3: RDD OPERATIONS =================\n")
    println(s"Preprocessed rows loaded from parquet: ${finalDf.count()}\n")

    // Keep only needed columns for analysis
    val analysisDf = finalDf
      .select("Category", "Rating", "Maximum Installs")
      .na.drop()

    println("Schema used for RDD analysis:")
    analysisDf.printSchema()
    println()

    // ============================================================
    // Analysis 1: Top Categories by Total Installs
    // Question: Which app categories achieve the highest total installs?
    //
    // Transformations used:
    // 1. map
    // 2. reduceByKey
    // 3. sortBy
    //
    // Actions used:
    // 1. count
    // 2. take
    // ============================================================

    println("========== Analysis 1: Top Categories by Total Installs ==========\n")

    val categoryInstallsRdd = analysisDf.rdd.map { row =>
      val category = row.getAs[String]("Category")
      val installsAny = row.getAs[Any]("Maximum Installs")

      val installs = installsAny match {
        case n: Long   => n
        case n: Int    => n.toLong
        case n: Double => n.toLong
        case n: Float  => n.toLong
        case s: String => s.toLong
        case _         => 0L
      }

      (category, installs)
    }

    val topCategoriesRdd = categoryInstallsRdd
      .reduceByKey(_ + _)
      .sortBy({ case (_, totalInstalls) => totalInstalls }, ascending = false)

    // Action 1
    val categoryGroupCount = topCategoriesRdd.count()
    println(s"Number of category groups after aggregation: $categoryGroupCount\n")

    // Action 2
    val top10Categories = topCategoriesRdd.take(10)

    println("Top 10 Categories by Total Installs:")
    top10Categories.zipWithIndex.foreach { case ((category, totalInstalls), idx) =>
      println(f"${idx + 1}%2d. $category%-35s -> $totalInstalls%,d")
    }

    println(
      "\nExplanation: This analysis uses RDD aggregation to compute the total number of installs " +
      "for each application category. It helps identify the categories with the strongest overall popularity.\n"
    )

    // ============================================================
    // Analysis 2: Rating vs Installs
    // Question: Do higher-rated apps tend to achieve more installs?
    //
    // Transformations used:
    // 4. filter
    // 5. groupByKey
    // (map is also reused here)
    //
    // Actions used:
    // 3. first
    // 4. collect
    // 5. foreach
    // ============================================================

    println("========== Analysis 2: Rating Buckets vs Average Installs ==========\n")

    def ratingBucket(rating: Double): String = {
      if (rating < 1.0) "0-1"
      else if (rating < 2.0) "1-2"
      else if (rating < 3.0) "2-3"
      else if (rating < 4.0) "3-4"
      else "4-5"
    }

    val ratingInstallRdd = analysisDf.rdd
      .filter { row =>
        val ratingAny = row.getAs[Any]("Rating")
        val rating = ratingAny match {
          case n: Double => n
          case n: Float  => n.toDouble
          case n: Int    => n.toDouble
          case n: Long   => n.toDouble
          case s: String => s.toDouble
          case _         => -1.0
        }

        rating >= 0.0 && rating <= 5.0
      }
      .map { row =>
        val ratingAny = row.getAs[Any]("Rating")
        val installsAny = row.getAs[Any]("Maximum Installs")

        val rating = ratingAny match {
          case n: Double => n
          case n: Float  => n.toDouble
          case n: Int    => n.toDouble
          case n: Long   => n.toDouble
          case s: String => s.toDouble
          case _         => 0.0
        }

        val installs = installsAny match {
          case n: Long   => n
          case n: Int    => n.toLong
          case n: Double => n.toLong
          case n: Float  => n.toLong
          case s: String => s.toLong
          case _         => 0L
        }

        val bucket = ratingBucket(rating)
        (bucket, installs)
      }
      .groupByKey()
      .map { case (bucket, installsIterable) =>
        val installsSeq = installsIterable.toSeq
        val appCount = installsSeq.size
        val avgInstalls =
          if (appCount == 0) 0.0
          else installsSeq.sum.toDouble / appCount.toDouble

        (bucket, appCount, avgInstalls)
      }
      .sortBy(_._1)

    // Action 3
    val firstBucketResult = ratingInstallRdd.first()
    println(s"First bucket result: $firstBucketResult\n")

    // Action 4
    val bucketResults = ratingInstallRdd.collect()

    println("Collected rating bucket results:")
    bucketResults.foreach { case (bucket, appCount, avgInstalls) =>
      println(f"Bucket: $bucket%-3s | Apps: $appCount%,d | Avg Installs: $avgInstalls%,.2f")
    }

    println()

    // Action 5
    println("foreach action output:")
    ratingInstallRdd.foreach(println)

    println(
      "\nExplanation: This analysis groups applications into rating ranges, then calculates the " +
      "number of apps and the average installs for each range. It helps evaluate whether higher-rated apps " +
      "are generally associated with greater popularity.\n"
    )

    // ============================================================
    // Requirement Check
    // ============================================================

    println("========== Requirement Check ==========\n")

    println("RDD Transformations used:")
    println("1. map")
    println("2. reduceByKey")
    println("3. sortBy")
    println("4. filter")
    println("5. groupByKey\n")

    println("RDD Actions used:")
    println("1. count")
    println("2. take")
    println("3. first")
    println("4. collect")
    println("5. foreach\n")

    println("================= PHASE 3 COMPLETED SUCCESSFULLY =================\n")

    spark.stop()
  }
}