package core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

// if the case class is inside the main method
// get task not serializable exception
// this being inside makes the SparkLogProcess not seriallizable

// if it's outside the main method in the object then too it is fine

// if it is defined in the main method only that causes task not szerialiable error
case class LogRecord(
    ipAddress: String,
    clientIdentity: String,
    userId: String,
    dateTime: String,
    method: String,
    endPoint: String,
    protocol: String,
    responseCode: String,
    contentSize: Long
)

object SparkLogProcessor {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("SparkLogProcessor")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    //  val AALP = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)\s*" (\d{3}) (\S+)""".r
    //val fileLoc = "D:/ufdata/apachelogs"
    val fileLoc = args(0)
    val logFileRDD = sc.textFile(fileLoc)
    println("No of logs " + logFileRDD.count)
    logFileRDD take 2 foreach println

    def parseApacheLogLineO(logLine: String): LogRecord = {
      val AALP =
        """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)\s*" (\d{3}) (\S+)""".r
      val res = AALP.findFirstMatchIn(logLine)
      if (res.isEmpty) {
        throw new RuntimeException("Cannot parse log line " + logLine)
      }
      val m = res.get
      LogRecord(
        m.group(1),
        m.group(2),
        m.group(3),
        m.group(4),
        m.group(5),
        m.group(6),
        m.group(7),
        m.group(8),
        m.group(9) match { case "-" => 0; case x => x.toLong }
      )
    }
    def parseApacheLogLine(logLine: String): LogRecord = {
      val AALP =
        """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)\s*" (\d{3}) (\S+)""".r
      val res = AALP.findFirstMatchIn(logLine)
      try {
        val m = res.get
        LogRecord(
          m.group(1),
          m.group(2),
          m.group(3),
          m.group(4),
          m.group(5),
          m.group(6),
          m.group(7),
          m.group(8),
          m.group(9) match { case "-" => 0; case x => x.toLong }
        )
      } catch {
        case ex: Exception => null
      }
    }

    val accessLogs =
      sc.textFile(fileLoc).map(parseApacheLogLine).filter(_ != null)

    accessLogs.persist(StorageLevel.MEMORY_ONLY_SER)
    accessLogs.first

    val content_sizes = accessLogs.map(_.contentSize).cache()
    val content_sizes_avg = content_sizes.sum / content_sizes.count
    val content_sizes_min = content_sizes.min
    val content_sizes_max = content_sizes.max
    printf(
      "Content size average: %d, Min: %d, Max: %d ",
      content_sizes_avg.toInt,
      content_sizes_min,
      content_sizes_max
    )

    val responseCodes = accessLogs
      .map(x => x.responseCode)
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._1)
    responseCodes.collect.foreach(println)
    responseCodes.take(2).toSet

    println("Any 20 hosts that have accessed more than 10 times:\n")
    val any20HostsMoreThan10 = accessLogs
      .map(x => (x.ipAddress, 1))
      .reduceByKey(_ + _)
      .filter(_._2 > 10)
      .sortBy(-_._2)
      .take(20)

    val topTenEndpoints = accessLogs
      .map(x => (x.endPoint, 1))
      .reduceByKey(_ + _)
      .sortBy(-_._2)
      .take(10)
    topTenEndpoints(0)._1

    // Unique host count
    val accessLogsIPAddressDistinct = accessLogs.map(_.ipAddress).distinct.count

    // No of unique hosts by day
    // get the unique hosts by day -
    // create a tuple of day and ip address
    // call distinct on it - group by key
    // map it to the first part - the day, second part size and sort by second part descending
    val dailyUniqueHosts = accessLogs
      .map(x => (x.dateTime.substring(0, 2).toInt, x.ipAddress))
      .distinct
      .groupByKey()
      .map(x => (x._1, x._2.size))
      .sortBy(_._1.toInt)
    dailyUniqueHosts.collect.foreach(println)

    // Average requests per host per day
    // first get the total number of requests for each day
    val dailyRequests = accessLogs
      .map(x => (x.dateTime.substring(0, 2).toInt, 1))
      .reduceByKey(_ + _)
    println("The daily requests in total")
    dailyRequests.collect.foreach(println)
    // join dailyUniqueHosts with dailyRequests - the key in each case will be the day
    // the value will be a two pair of number of unique hosts and total requests
    // divide the total requests by the unique hosts to get the average daily request per host
    // using tuple notation
    println("using tuple notation")
    dailyUniqueHosts
      .join(dailyRequests)
      .map(x => (x._1, x._2._2 / x._2._1))
      .sortBy(_._1)
      .collect
      .foreach(println)
    println("using case notation")
    val avgHostsRequestPerDay = dailyUniqueHosts
      .join(dailyRequests)
      .map {
        case (day, (hosts, requests)) => (day, (requests / hosts).toInt)
      }
      .sortBy(_._1)
      .collect

    // find out the bad records the ones which got the response code 404
    val badRecords = accessLogs.filter(_.responseCode == "404").cache
    println(badRecords.count)

    // bad hosts
    val frequentBadHosts =
      badRecords.map(x => (x.ipAddress, 1)).reduceByKey(_ + _).sortBy(-_._2)
    frequentBadHosts.take(5).foreach(println)

    // find out the 5 most frequent endponts
    val frequentBadEndPoints = badRecords
      .map(x => (x.endPoint, 1))
      .reduceByKey(_ + _)
      .sortBy(-_._2)
      .take(5)
    frequentBadEndPoints.foreach(println)
  }
}
