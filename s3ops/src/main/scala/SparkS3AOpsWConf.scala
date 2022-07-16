import org.apache.spark.sql.SparkSession
// works from the ide - sbt assembly not required
// from the command line and later with kubernetes
// need to copy hadoop-aws and aws-java-sdk-bundle jars to spark jars
// directory and stuff works without the packages bit
object SparkS3AOpsWConf {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Sparks3Kube").getOrCreate
    val sc = spark.sparkContext
    if (args(2) != "") sc.setLogLevel(args(2))
    // sc.setLogLevel("INFO")
    val awsAccessKeyId = sys.env("AWS_ACCESS_KEY_ID")
    val awsSecretKey = sys.env("AWS_SECRET_ACCESS_KEY")
    val s3FilePath = args(0)
    val s3WritePath = args(1)
    println(
      s"info setup is accesskey $awsAccessKeyId and secret key $awsSecretKey and s3FilePath $s3FilePath and  writepath $s3WritePath"
    )
    // sc.hadoopConfiguration.set("fs.s3a.endpoint.region", "us-east-1")
    sc.hadoopConfiguration.set("fs.s3a.access.key", awsAccessKeyId)
    sc.hadoopConfiguration.set("fs.s3a.secret.key", awsSecretKey)
    // sc.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
    sc.hadoopConfiguration.set("spark.fs.s3a.path.style.access", "true")
    sc.hadoopConfiguration
      .set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    val s3RDD = sc.textFile(s3FilePath)
    (41 to 50).foreach(x => println(s"line no $x"))
    s3RDD.collect.foreach(println)
    val wordCounts = s3RDD
      .flatMap(_ split "")
      .filter(_ != "")
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(-_._2)
    wordCounts.saveAsTextFile(s3WritePath)
    sc.stop()
    spark.stop()
  }

}
