package structured

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import scala.reflect.api.materializeTypeTag
//import com.mysql.jdbc.Driver

object KafkaStockStreamAggJDBC {
  /*
   * empty the checkpoint directory
   * if need be can create nsecmdpart afresh
   * run this
   * run FileBasedKafkaPartProducer
   * Track nsecmdpart through kafka-console-consumer
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("StructuredKafkaWordCount")
      .config("spark.sql.shuffle.partitions", "3")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    addStreamingQueryListeners(spark, true)

    val stockQuotes =
      spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args(0))
        .option("subscribe", args(1))
        .option("failOnDataLoss", "false")
        .option("startingOffsets", "latest")
        // we could explicitly set offsets
        //      .option("startingOffsets", endingOffsetsToBeginWith)
        //      .option("auto.offset.reset", "latest")
        .load()
        .select("KEY", "VALUE", "TIMESTAMP")

    val stocksDF = stockQuotes
      .as[(String, String, Timestamp)]
      .map(_._2 split ",")
      .map(x => Stock(x(0), x(2).toDouble, x(8).toInt, x(4).toDouble))
      .toDF

    val stocksAggregated = stockQuotes
      .as[(String, String, Timestamp)]
      .map(x => (x._2.split(","), x._3))
      .map(x =>
        (x._1(0), x._1(5).toDouble, x._1(8).toLong, x._1(9).toDouble, x._2)
      )
      .toDF("symbol", "close", "qty", "value", "tstamp")
      .withWatermark("tstamp", "30 seconds")
      .groupBy($"symbol")
      .agg(
        avg($"qty").as("avgqty"),
        avg($"value").as("avgval"),
        sum($"qty").as("totqty"),
        sum($"value").as("sumval"),
        min($"close").as("mincls"),
        max($"close").as("maxcls")
      )
      .toDF(
        "symbol",
        "avgqty",
        "avgval",
        "totqty",
        "sumval",
        "mincls",
        "maxcls"
      )
      .filter("""symbol = 'ITC' or symbol = 'TCS' or symbol = 'CIPLA' or
      symbol = 'AUROPHARMA' or symbol='BHARTIARTL'""")
      .coalesce(1)

    val writer =
      new JDBCSink(args(2), args(3), args(4))

    stocksAggregated.writeStream
      .foreach(writer)
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
      .awaitTermination()
    /*
     * mysql table creation
     * create table stockstats(symbol varchar(100), avgqty double, avgval double, totqty int, sumval double, mincls double, maxcls double);
     * create user 'kuser'@'master.e4rlearning.com' identified by 'ramanShastri24!';
     * grant all privileges on testdb.* to 'kuser'@'master.e4rlearning.com' with grant option;
     * flush privileges;
     */

    /*
    spark-submit --class structured.KafkaStockStreamAggJDBC <the uber jar> masterL9092 nsecmdpart jdbcLmysql:master.e4rlearning.com:3306  kuser ramanShastri24!
    or with --packages and the packaged jar
     */
  }
}
