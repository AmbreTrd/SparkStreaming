import org.apache.spark.sql.SparkSession

object Projet extends App {
  val sparkSession = SparkSession.builder()
    .appName("firstTry")
    .master("local[*]")
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

  sparkSession.conf.set("spark.sql.shuffle.partitions","5")

  /**
   * STATIC
   */

  val supermarket = sparkSession
    .read.format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load("data/supermarket_sales.csv")

  supermarket.printSchema()


  /**
   * STREAMING
   */

  // Récupérer le schema static

  val supermarketSchema = supermarket.schema

  // Lecture en streaming

  val supermarketStream = sparkSession
    .readStream
    .schema(supermarketSchema)
    .format("csv")
    .option("maxFilesPerTrigger","1")
    .option("header","true")
    .load("data/supermarket_sales.csv")

  println("Spark is streaming " + supermarketStream.isStreaming)

  /**
   * SELECT EXPRESSION
   */

"""  val onTimeAndLateTrains = supermarketStream
    .selectExpr(
      "Period",
      "Departure station"
    )
    .groupBy("Departure station")
    .sum()

"""

  /**
   * WRITE STREAM
   */

  supermarketStream


}
