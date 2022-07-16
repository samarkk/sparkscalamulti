package structured

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, max, min, sum}
import org.apache.spark.sql.streaming.Trigger

object StructuredCheckpointCheck {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("KafkaStockAggregation")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val simpleAgg =
      spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args(0))
        .option("subscribe", args(1))
        .option("startingffsets", "earliest")
        .load()
        .select("KEY", "VALUE")

    val simpleDF = simpleAgg
      .as[(String, String)]
      .map(_._2 split ",")
      .map(x => (x(0), x(1).toInt, x(2).toInt))
      .toDF("key", "iter", "nmbr")

    val aggDF = simpleDF
      .groupBy($"key")
      .agg(
        sum($"iter").as("count"),
        sum($"nmbr").as("sum")
      )

    val aggDFQuery = aggDF.writeStream
      .format("console")
      .outputMode("update")
      .trigger(Trigger.ProcessingTime(args(2) + " seconds"))
      .option("checkpointLocation", args(3))
      .option("truncate", false)
      .start

    aggDFQuery.awaitTermination()
  }
}
