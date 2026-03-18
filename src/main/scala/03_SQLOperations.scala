import org.apache.spark.sql.SparkSession

object GooglePlaySQLPhase4 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("IT462 - Google Play Phase 4 SQL")
      .master("local[*]")
      .getOrCreate()

      spark.sparkContext.setLogLevel("ERROR")

    // ============================================================
    // 1) Convert preprocessed dataset to DataFrame
    // ============================================================
    val df = spark.read.parquet("data/transformed/transformed_google_play.parquet")

    println(s"Dataset loaded successfully.")
    println(s"Rows: ${df.count()}, Columns: ${df.columns.length}")
    println("Columns:")
    println(df.columns.mkString(", "))

    // ============================================================
    // 2) Register as a temporary view
    // ============================================================
    df.createOrReplaceTempView("googleplay")
    println("Temporary view 'googleplay' created successfully.")

    // ============================================================
    // 3) Query 1: Top Categories by Average Installs
    // Question:
    // Which application categories achieve the highest average number of installs?
    // ============================================================
    val q1 = spark.sql("""
      SELECT
          Category,
          COUNT(*) AS total_apps,
          ROUND(AVG(`Maximum Installs`), 0) AS avg_installs
      FROM googleplay
      GROUP BY Category
      ORDER BY avg_installs DESC
      LIMIT 10
    """)

    println("\n========== Query 1: Top Categories by Average Installs ==========")
    q1.show(10, false)

    println(
  "\nExplanation: This query identifies the application categories with the highest average number of installs. " +
  "By grouping applications by category and computing the average installs, the analysis highlights the categories " +
  "where applications tend to attract more installs on the Google Play Store.\n"
)

    // ============================================================
    // 4) Query 2: Rating vs Success
    // Question:
    // How does application rating relate to application success in terms of installs?
    // ============================================================
    val q2 = spark.sql("""
      SELECT
          ROUND(Rating) AS rating_level,
          COUNT(*) AS total_apps,
          ROUND(AVG(`Maximum Installs`), 0) AS avg_installs
      FROM googleplay
      WHERE Rating IS NOT NULL
      GROUP BY ROUND(Rating)
      ORDER BY rating_level DESC
    """)

    println("\n========== Query 2: Rating vs Success ==========")
    q2.show(false)
    println(
  "\nExplanation: This query examines the relationship between application ratings and success. " +
  "Applications are grouped by rating level, and the average installs are calculated for each group. " +
  "This helps determine whether higher-rated applications tend to attract more installs.\n"
)

    // ============================================================
    // 5) Query 3: High Rating Categories with HAVING
    // Question:
    // Which categories maintain high average ratings among categories
    // with a sufficient number of applications?
    // ============================================================
    val q3 = spark.sql("""
      SELECT
          Category,
          COUNT(*) AS app_count,
          ROUND(AVG(Rating), 2) AS avg_rating
      FROM googleplay
      WHERE Rating IS NOT NULL
      GROUP BY Category
      HAVING COUNT(*) >= 100
      ORDER BY avg_rating DESC, app_count DESC
      LIMIT 10
    """)

    println("\n========== Query 3: High Rating Categories with HAVING ==========")
    q3.show(10, false)

    println(
  "\nExplanation: This query identifies categories that maintain relatively high average ratings while " +
  "ensuring that only categories with a sufficient number of applications are considered. " +
  "The HAVING clause filters categories with at least 100 applications, making the comparison more reliable.\n"
)

    // ============================================================
// 6) Query 4: Free vs Paid Apps Success
// Question:
// Do free applications achieve higher average installs than
// paid applications?
// ============================================================

val q4 = spark.sql("""
  SELECT
      CASE
          WHEN Price = 0 THEN 'Free'
          ELSE 'Paid'
      END AS app_type,
      COUNT(*) AS total_apps,
      ROUND(AVG(`Maximum Installs`), 2) AS avg_installs
  FROM googleplay
  GROUP BY
      CASE
          WHEN Price = 0 THEN 'Free'
          ELSE 'Paid'
      END
  ORDER BY avg_installs DESC
""")

println("\n========== Query 4: Free vs Paid Apps Success ==========")
q4.show(10, false)
println(
  "\nExplanation: This query compares the success of free and paid applications by calculating the average " +
  "number of installs for each group. The results help determine whether pricing influences application " +
  "adoption and whether free applications tend to achieve higher install counts.\n"
)

    // ============================================================
    // 7) Query 5: App Age vs Success
    // Question:
    // How does application age affect application success in terms of installs?
    // ============================================================
    val q5 = spark.sql("""
      SELECT
          CASE
              WHEN app_age_days < 365 THEN 'Less than 1 year'
              WHEN app_age_days >= 365 AND app_age_days < 1095 THEN '1 to 3 years'
              WHEN app_age_days >= 1095 AND app_age_days < 1825 THEN '3 to 5 years'
              ELSE 'More than 5 years'
          END AS app_age_group,
          COUNT(*) AS total_apps,
          ROUND(AVG(`Maximum Installs`), 0) AS avg_installs
      FROM googleplay
      WHERE app_age_days IS NOT NULL
      GROUP BY
          CASE
              WHEN app_age_days < 365 THEN 'Less than 1 year'
              WHEN app_age_days >= 365 AND app_age_days < 1095 THEN '1 to 3 years'
              WHEN app_age_days >= 1095 AND app_age_days < 1825 THEN '3 to 5 years'
              ELSE 'More than 5 years'
          END
      ORDER BY avg_installs DESC
    """)

    println("\n========== Query 5: App Age vs Success ==========")
    q5.show(false)

    println(
      "\nExplanation: This query analyzes how application age affects success by dividing applications into age groups " +
      "and calculating the average installs for each group. This provides a clearer view of whether older applications " +
      "tend to achieve higher install counts than newer ones.\n"
    )

    // ============================================================
    // 8) Query 6: Top Content Rating by Success within Each Category
    // Question:
    // Within each application category, which content rating achieves
    // the highest average number of installs?
    // ============================================================
    val q6 = spark.sql("""
      WITH content_rating_stats AS (
          SELECT
              Category,
              `Content Rating`,
              COUNT(*) AS total_apps,
              ROUND(AVG(`Maximum Installs`), 0) AS avg_installs
          FROM googleplay
          WHERE `Content Rating` IS NOT NULL
          GROUP BY Category, `Content Rating`
      ),
      ranked_content_ratings AS (
          SELECT
              Category,
              `Content Rating`,
              total_apps,
              avg_installs,
              ROW_NUMBER() OVER (
                  PARTITION BY Category
                  ORDER BY avg_installs DESC
              ) AS rank_num
          FROM content_rating_stats
      )
      SELECT
          Category,
          `Content Rating`,
          total_apps,
          avg_installs
      FROM ranked_content_ratings
      WHERE rank_num = 1
      ORDER BY avg_installs DESC
      LIMIT 15
    """)

    println("\n========== Query 6: Top Content Rating by Success within Each Category ==========")
    q6.show(15, false)

    println(
      "\nExplanation: This query determines, within each application category, which content rating achieves " +
      "the highest average number of installs. A window function is used to rank content ratings inside each " +
      "category and keep only the top-performing one.\n"
    )

    // ============================================================
    // 9) Query 7: Statistical Summary
    // Question:
    // What are the overall statistical characteristics of application
    // price and installs in the dataset?
    // ============================================================
    val q7 = spark.sql("""
      SELECT
          COUNT(*) AS total_apps,
          COUNT(DISTINCT Category) AS distinct_categories,
          ROUND(AVG(Price), 2) AS avg_price,
          ROUND(VARIANCE(Price), 2) AS price_variance,
          ROUND(AVG(`Maximum Installs`), 2) AS avg_installs,
          ROUND(VARIANCE(`Maximum Installs`), 2) AS installs_variance,
          percentile_approx(Price, 0.50) AS median_price,
          percentile_approx(`Maximum Installs`, 0.25) AS installs_p25,
          percentile_approx(`Maximum Installs`, 0.50) AS median_installs,
          percentile_approx(`Maximum Installs`, 0.75) AS installs_p75
      FROM googleplay
      WHERE Price IS NOT NULL
        AND `Maximum Installs` IS NOT NULL
    """)

    println("\n========== Query 7: Statistical Summary ==========")
    q7.show(false)

    println(
      "\nExplanation: This query provides an overall statistical summary of application price and installs. " +
      "It reports averages, variance, median values, and install percentiles to describe the distribution " +
      "and spread of application pricing and success in the dataset.\n"
    )
    spark.stop()
  }
}

