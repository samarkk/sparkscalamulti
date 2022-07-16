package dstream
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object KafkaWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingKafkaWC")
    val ssc = new StreamingContext(conf, Seconds(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> args(0),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "wcdircheck",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(args(1))
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val streamCounts = stream
      .map(record => record.value)
      .flatMap(line => line.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey((first, second) => first + second)
    streamCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
