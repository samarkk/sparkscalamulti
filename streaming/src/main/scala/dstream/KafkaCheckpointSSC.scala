package dstream

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

// pipe the output of spark-submit to some file lets say sparklog.txt
// and use this to figure out checkpoint and resume from where one left off
// so from the producer issue stuff such as a,1,1, a,1,2, a,1,3
// which should give a total of a,3,6

// then stop the app
// and puch in some more

// if we have checkpoint enabled then the ones which were produced
// when the app was down are going to be accounted for

// remember to use auto.offset.reset as earliest

object KafkaCheckpointSSC {
  // Function to create and setup a new StreamingContext
  // args - 6 0-bootstrap server, 1 - topic, 2 -checkpoint location
  // 3 - streaming context duration, 4 - group id, 5 - auto.offset.reset
  // 6 -log level
  def main(args: Array[String]): Unit = {
    val checkpointDirectory = args(2)

    def functionToCreateContext(): StreamingContext = {
      val conf = new SparkConf().setAppName("KafkaCheckpointSSC")
      val ssc = new StreamingContext(conf, Seconds(Integer.parseInt(args(3))))
      ssc.sparkContext.setLogLevel(args(6))
      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> args(0),
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> args(4),
        "auto.offset.reset" -> args(5),
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )

      val topics = Array(args(1))
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

      // this aint' of much help in identifying
//      stocksAgg.foreachRDD(rdd => {
//        if (rdd.count() > 0) {
//          rdd.saveAsTextFile(args(7))
//        } else {
//          println("nope for this batch nothing to show")
//        }
//      })

      stocksAgg.print()

      ssc.checkpoint(checkpointDirectory) // set checkpoint directory
      ssc
    }

    val context =
      StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext)
    context.start()
    context.awaitTermination()
  }
}
