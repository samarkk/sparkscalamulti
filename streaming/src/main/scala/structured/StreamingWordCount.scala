package structured

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{
  current_timestamp,
  explode,
  split,
  window
}

object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    // args
    // 0 - hive metastore uris, 1 - host on which nc is running , 2 - port of nc
    // 3 - hdfs location where watermark output will be written, 4 - hdfs checkpoint location

    // first run plain word count - uncomment val wordCount = ... and val query = wordCount.
    // map("sco " +) - since we have output mode complete we will get in each batch counts for
    // everything that would have been punched in to the console till that time

    // then comment out wordcounts and query and uncomment windowed word counts and
    // windowed query and run

    // then sequentially we can go on, uncomment watermarkedwindow counts and query
    // and finally the batch table operations where we can join with existing data
    // in this case a demo wimptable and kind of add on metadata info to the streaming
    // operations
    val spark = SparkSession
      .builder()
      .appName("StreamingWordCount")
      .config("hive.metastore.uris", args(0))
      .config("spark.sql.shuffle.partitions", "5")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", args(1))
      .option("port", args(2))
      .load()
      .withColumn("timestamp", current_timestamp())

    val words = lines
      .select(explode(split($"value", " ")).as("word"), $"timestamp")
      .filter("word <> ''")
    //
    val wordCounts = words
      .groupBy($"word")
      .count
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()

    wordCounts.awaitTermination()

    val windowedWordCounts = words
      .groupBy(window($"timestamp", "20 seconds", "10 seconds"), $"word")
      .count()

    val windowQuery = windowedWordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .start()

    windowQuery.awaitTermination()

    val waterMarkedWindowCount = words
      .withWatermark("timestamp", "5 minutes")
      .groupBy(window($"timestamp", "5 minutes", "1 minutes"), $"word")
      .count
      .toDF("wdw", "word", "counts")
    //  waterMarkedWindowCount.printSchema()

    // we can see watermarking operations also
    // the data is in the wccheck table in default hive db
    // we have a watermark interval 5 minutes and window of 5 minutes sliding every minute
    // so data punched in at 1715 will be available at 1726 as 1726 - (1715 +5) = 6 > 5
    // the watermarking duration
    val waterMarkQuery = waterMarkedWindowCount.writeStream
      .format("parquet")
      .outputMode("append")
      .option("truncate", false)
      .option(
        "path",
        args(3)
      )
      .option(
        "checkpointLocation",
        args(4)
      )
      //          .trigger(ProcessingTime("10 seconds"))
      .start()

    waterMarkQuery.awaitTermination()

    val batchTbl = spark.read.table("wimptbl")
    //  //  batchTbl.show()
    //  println("words schema")
    //  words.printSchema()
    //  println("words left outer join batchtbl schema")
    //  words.join(batchTbl, Seq("word"), "left_outer").printSchema()
    //
    val batchTableQuery = words
      .join(batchTbl, Seq("word"), "left_outer")
      .na
      .fill(0, Seq("freq"))
      .groupBy("word", "freq")
      //    .groupBy(window($"timestamp", "10 seconds", "5 seconds"), $"word", $"freq")
      .count()
      .withColumn("wtdfreq", $"freq" * $"count")
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()

    batchTableQuery.awaitTermination()
  }
}
