import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{window, column, desc, col}

object LifeWorkDeath extends App {

  val sparkSession = SparkSession.builder()
    .appName("Stream")
    .master("local[*]")
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

  sparkSession.conf.set("spark.sql.shuffle.partitions","5")

  /**
   * STATIC
   */

  val lifeWorkDeath = sparkSession
    .read.format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load("data/AgeDataset.csv")

  lifeWorkDeath.printSchema()
  lifeWorkDeath.show(truncate = false)

  lifeWorkDeath.createOrReplaceTempView("age_table")


  /**
   * STREAMING
   */

  // Récupérer le schema static

  val lifeWorkDeathSchema = lifeWorkDeath.schema

  // Lecture en streaming

  val lifeWorkDeathStream = sparkSession
    .readStream
    .schema(lifeWorkDeathSchema)
    .format("csv")
    .option("maxFilesPerTrigger","1")
    .option("header","true")
    .load("data/AgeDataset.csv")

  println("Spark is streaming " + lifeWorkDeathStream.isStreaming)

  /**
   * ANALYSES
   */

  // Répartition de Gender
"""
  val genderRepartition = lifeWorkDeathStream
    .selectExpr(
      "Id", "Gender"
    )
    .groupBy("Gender")
    .sum()
  genderRepartition.show()
"""

  val query = lifeWorkDeathStream.groupBy("Gender").count()

  query.writeStream
    .outputMode("complete")
    .format("console")
    .start()
    .awaitTermination()


  /**
   * WRITE STREAM
   */


}
