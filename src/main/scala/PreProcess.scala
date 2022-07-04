import org.apache.spark.sql.SparkSession

object PreProcess extends App {

  /*
  Code qui sert à diviser les donnnées pour avoir plusieurs fichiers,
  avec un fichier = un mois de données
   */
  val sparkSession = SparkSession.builder()
    .appName("firstTry")
    .master("local[*]")
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

  sparkSession.conf.set("spark.sql.shuffle.partitions","5") // Du mal à comprendre le shuffle

  /**
   * STATIC
   */

  val transport = sparkSession
    .read.format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load("data/Regularities_by_liaisons_Trains_France.csv")

  transport.printSchema()
  transport.show(truncate = false)

  transport
    .write
    .partitionBy("Period")
    .mode("overwrite")
    .csv("data/by-month")

}
