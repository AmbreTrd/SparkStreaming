import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object Projet extends App {
  val sparkSession = SparkSession.builder()
    .appName("firstTry")
    .master("local[*]")
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

  sparkSession.conf.set("spark.sql.shuffle.partitions","5") // Du mal à comprendre le shuffle

  val transport = sparkSession
    .read.format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load("data/Regularities_by_liaisons_Trains_France.csv")
  // TODO : Diviser la donnée en amont du lancement du code ?

  transport.printSchema()
  transport.show(truncate = false)

  transport.createOrReplaceTempView("retail_table")


  /**
   * Streaming
   */

  // Récupérer le schema static

  val transportSchema = transport.schema

  // Lecture en streaming

  val transportStream = sparkSession
    .readStream
    .schema(transportSchema)
    .format("csv")
    .option("maxFilesPerTrigger","1")
    .option("header","true")
    .load("data/Regularities_by_liaisons_Trains_France.csv")

  println("Spark is streaming " + transportStream.isStreaming)


}
