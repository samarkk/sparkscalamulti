package dstream

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaStocksAgg {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingKafkaStocksAgg")
    val ssc = new StreamingContext(conf, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "master:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "g2",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("myqtopic")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val stocksAgg = stream
      .map(record => record.value)
      .map(stockline => stockline.split(","))
      .map(strec => (strec(0), (strec(1).toInt, strec(2).toDouble)))
      .reduceByKey {
        case ((q1, v1), (q2, v2)) => (q1 + q2, v1 + v2)
      }

    stocksAgg.print()
//      .flatMap(strec => strec.value.split(","))
//      .map(rec => (rec(0), (rec(8), rec(9))))

//    val streamCounts = stream
//      .map(record => record.value)
//      .flatMap(line => line.split("\\s+"))
//      .map(word => (word, 1))
//      .reduceByKey((first, second) => first + second)
//    streamCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
