import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object BatchSalesPipeline {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("E-Commerce Daily Sales Batch Pipeline")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try {
      val basePath = "src/main/resources/data/"

      val transactionSchema = StructType(Seq(
        StructField("transaction_id", StringType, true),
        StructField("customer_id", StringType, true),
        StructField("product_id", StringType, true),
        StructField("transaction_date", StringType, true),
        StructField("quantity", StringType, true),
        StructField("unit_price", StringType, true)
      ))

      val transactions = spark.read
        .option("header", "true")
        .schema(transactionSchema)
        .csv(basePath + "transactions.csv")

      val customers = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(basePath + "customers.csv")

      val products = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(basePath + "products.csv")

      println("=== Raw Transactions ===")
      transactions.show(false)
      println(s"Raw transaction count: ${transactions.count()}")

      val typedTransactions = transactions
        .withColumn("quantity_num", col("quantity").cast(IntegerType))
        .withColumn("unit_price_num", col("unit_price").cast(DoubleType))
        .withColumn("sale_date", to_date(col("transaction_date"), "yyyy-MM-dd"))

      val validTransactions = typedTransactions
        .filter(
          col("transaction_id").isNotNull &&
          trim(col("transaction_id")) =!= "" &&
          col("customer_id").isNotNull &&
          trim(col("customer_id")) =!= "" &&
          col("product_id").isNotNull &&
          trim(col("product_id")) =!= "" &&
          col("sale_date").isNotNull &&
          col("quantity_num").isNotNull &&
          col("quantity_num") > 0 &&
          col("unit_price_num").isNotNull &&
          col("unit_price_num") > 0
        )
        .drop("quantity", "unit_price")
        .withColumnRenamed("quantity_num", "quantity")
        .withColumnRenamed("unit_price_num", "unit_price")

      println("=== Valid Transactions ===")
      validTransactions.show(false)
      println(s"Valid transaction count: ${validTransactions.count()}")
      println(s"Rejected transaction count: ${transactions.count() - validTransactions.count()}")

      val enriched = validTransactions
        .join(customers, Seq("customer_id"), "inner")
        .join(products, Seq("product_id"), "inner")
        .withColumn("revenue", col("quantity") * col("unit_price"))

      println("=== Enriched Transactions ===")
      enriched.show(false)

      val dailySales = enriched
        .groupBy(
          col("sale_date"),
          col("city"),
          col("category")
        )
        .agg(
          countDistinct("transaction_id").as("total_transactions"),
          sum("quantity").as("total_quantity"),
          round(sum("revenue"), 2).as("total_revenue")
        )

      println("=== Daily Sales Summary ===")
      dailySales.orderBy("sale_date", "city", "category").show(false)

      val outputPath = "output/daily_sales"
      dailySales
        .withColumn("year", year(col("sale_date")))
        .withColumn("month", month(col("sale_date")))
        .withColumn("day", dayofmonth(col("sale_date")))
        .write
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(outputPath)

      println(s"Partitioned Parquet output written to: $outputPath")

      println("=== Read Back Parquet Output ===")
      spark.read.parquet(outputPath).show(false)
    } finally {
      spark.stop()
    }
  }
}
