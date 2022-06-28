import org.apache.spark.sql.SparkSession

object Projet extends App {
  val sparkSession = SparkSession.builder()
    .appName("firstTry")
    .master("local[*]")
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

  val transport = sparkSession
    .read
    .options(Map("inferSchema"->"true","header"->"true"))
    .csv("data/Regularities_by_liaisons_Trains_France.csv")

  transport.printSchema()
  transport.show(truncate = false)



  """
  studentData.show()

  studentData.createOrReplaceTempView("retail_tabeul")
  val tabdf = sparkSession.sql("select InvoiceNo, InvoiceDate from retail_tabeul")

  tabdf.show()

"""


  /*
  val maxPurchase = retailDF.selectExpr(
  "CustomerID",
  "(UnitPrice * Quantity) as total_cost",
  "InvoiceDate")
  .groupBy(
    col("CustomerID"),
    window(col("InvoiceDate"), "1 hour") as "invoiceDate"
  ).sum("total_cost")
   */
  /*
    System.in.read
    sparkSession.stop()

    /**
     * Streaming
     */
    // Récuperation le schema static
    val retailschema = retailDF.schema
    // Lecture en streaming
    val retailStream = sparkSession
      .readStream
      .schema(retailschema)
      .format("csv")
      .option("maxFilesPerTrigger","1")
      .option("header","true")
      .load("/Users/yacine/IdeaProjects/ESGI-spark-streaming/data/by-day/*.csv")
    println("spark is streaming " + retailStream.isStreaming)
    val maxPurchasePerHour = retailStream.selectExpr(
      "CustomerID",
      "(UnitPrice * Quantity) as total_cost",
      "InvoiceDate")
      .groupBy(
        col("CustomerID"),
        window(col("InvoiceDate"), "1 hour") as "invoiceDate"
      ).sum("total_cost")
    // Streaming Action
    maxPurchasePerHour.writeStream
      .format("memory") // memory = store in-memory table
      .queryName("customer_table")  // the name of the in-memory table
      .outputMode("complete") // complete = all the counts should be in the table
      .start
    println(sparkSession.streams.active)
    for (i <- 1 to 50) {
      sparkSession.sql(
        """
          |Select * from customer_table
          |ORDER BY `sum(total_cost)` DESC
          |""".stripMargin
      ).show(false)
      Thread.sleep(1000)
    }
    System.in.read
    sparkSession.stop()
    */
   */

  /* POUR CONCATENER DEUX DATAFRAMES D'UN STREAM

  val impressions = sparkSession
    .readStream
    .format("rate") // rate génere des données en streaming avec deux colonnes:
                            // col1 = value : incrémental (0,1,2....)
                            // col2 = timestamp : temps de la génération de la données
                            // | value | timestamp |
                            // ---------------------
                            // | 0     | 01-06-2022 14:14:32-234 |
    .option("rowsPerSecond", "5")  // genere 5 lignes par seconde
    .option("numPartitions", "1") // nombre de partitions du DF
    .load()
    .select(
      col("value").as("adId"),
      col("timestamp").as("impressionTime")
    )
   */


}
