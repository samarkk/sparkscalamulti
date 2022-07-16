package structured

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, max, min, sum}
import org.apache.spark.sql.streaming.Trigger

object KafkaStockStreamAggregated {
  /*
   * run this app
   * and run testkfp.sh - refer to that program in scripts for
   * what it does and how to run it
   * the difference between this and KafkaStockStreamKafka is
   * the streaming aggregations are written to a kafka topic there
   *
   */
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("KafkaStockAggregation")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val stockQuotes =
      spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args(0))
        .option("subscribe", args(1))
        .option("startingffsets", "earliest")
        .load()
        .select("KEY", "VALUE")

    val stocksDF = stockQuotes
      .as[(String, String)]
      .map(_._2 split ",")
      .map(x => Stock(x(0), x(2).toDouble, x(8).toInt, x(4).toDouble))
      .toDF

    val stocksAggregated = stocksDF
      .groupBy($"symbol")
      .agg(
        avg($"qty").as("avgqty"),
        avg($"value").as("avgval"),
        sum($"qty").as("totqty"),
        sum($"value").as("sumval"),
        min($"close").as("mincls"),
        max($"close").as("maxcls")
      )

    val stocksAggregatedQuery = stocksAggregated.writeStream
      .format("console")
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("truncate", false)
      .start
  }
}
