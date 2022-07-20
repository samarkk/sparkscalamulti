package supchain

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object ExploreCleanData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SCExamples")
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext
    //  println(sc.version + " , " + spark.version)
    sc.setLogLevel("ERROR")
    // analysing the data - centrality and deviation

    val carsDFPath = args(0)

    // load the cars csv dataset
    val carsDF = spark.read
      .option("inferSchema", true)
      .option("header", true)
      .csv(carsDFPath)

    case class Car(
        name: String,
        sports_car: Boolean,
        suv: Boolean,
        wagon: Boolean,
        minivan: Boolean,
        pickup: Boolean,
        all_wheel: Boolean,
        rear_wheel: Boolean,
        Price: Int,
        Dealer_Cost: Int,
        Engine_size: Double,
        cylinders: Int,
        horsepower: Int,
        city_miles_per_gallon: String,
        highway_miles_per_gallon: String,
        weight: String,
        base_wheel: String,
        lngth: String,
        width: String
    )

    val carsRDD = sc
      .textFile(carsDFPath)
      .map(x => x.split(","))
      .filter(x => x(0) != "name")
      .map(x =>
        Car(
          x(0),
          x(1).toBoolean,
          x(2).toBoolean,
          x(3).toBoolean,
          x(4).toBoolean,
          x(5).toBoolean,
          x(6).toBoolean,
          x(7).toBoolean,
          x(8).toInt,
          x(9).toInt,
          x(10).toDouble,
          x(11).toInt,
          x(12).toInt,
          x(13),
          x(14),
          x(15),
          x(16),
          x(17),
          x(18)
        )
      )

    // the mean
    carsDF.select(avg($"Price")).show
    // the median
    // the column, the quantile and the relative error - smaller the
    // number higher the compute intensity and time that will be taken
    carsDF.stat.approxQuantile(Array("Price"), Array(0.5), 0.02)
    // find 10,20,30,40 quantile
    // to check along with the quantile
    (for (x <- 1 to 10) yield x / 10.0).map(x =>
      (x, carsDF.stat.approxQuantile("Price", Array(x), 0.02))
    )

    // some fields were inferred as String type
    // because of non number values
    // therefore we have to enforce schema ourselves

    val carsSchema = carsDF.schema

    val carsSelfSchema = StructType(
      Array(
        StructField("name", StringType, true),
        StructField("sports_car", BooleanType, true),
        StructField("suv", BooleanType, true),
        StructField("wagon", BooleanType, true),
        StructField("minivan", BooleanType, true),
        StructField("pickup", BooleanType, true),
        StructField("all_wheel", BooleanType, true),
        StructField("rear_wheel", BooleanType, true),
        StructField("Price", IntegerType, true),
        StructField("Dealer_Cost", IntegerType, true),
        StructField("Engine_size", DoubleType, true),
        StructField("cylenders", IntegerType, true),
        StructField("horsepower", IntegerType, true),
        StructField("city_miles_per_galloon", IntegerType, true),
        StructField("highway_miles_per_Gallon", IntegerType, true),
        StructField("weight", StringType, true),
        StructField("base_wheeel", IntegerType, true),
        StructField("length", IntegerType, true),
        StructField("width", IntegerType, true)
      )
    )

    // read the data frame with own created schema
    val carsDFWithSchema =
      spark.read.option("header", true).schema(carsSelfSchema).csv(carsDFPath)

    // find the number of null values
    carsDFWithSchema
      .select(
        carsDFWithSchema.columns
          .map(c => sum(col(c).isNull.cast("int")).alias(c)): _*
      )
      .show

    // confused about the part isNull.cast("int") - where ti is null it is
    // cast as integer - default value 1 ? thats how sum gets the count
    // this should help with the confusion
    carsDFWithSchema
      .select(
        $"name",
        $"Price",
        $"city_miles_per_galloon",
        $"city_miles_per_galloon".isNull.cast("int").alias("testcol")
      )
      .filter("testcol = 1")
      .show

    val df = carsDFWithSchema

    // mean
    df.select(mean("width"), mean("horsepower"), stddev("width")).show

    // mode
    df.groupBy("engine_size").count().orderBy(desc("count")).first.toSeq

//    df.select("horsepower", "width").columns.map(mean).mkString(",")
//    val res20: String = avg(horsepower),avg(width)

    // unfold the array
    df.select(df.select("horsepower", "width").columns.map(mean): _*).show
    // replacing the na values with averages in the two columns horsepower and width
    df.na.fill(
      Array("horsepower", "width")
        .zip(
          df.select(df.select("horsepower", "width").columns.map(mean): _*)
            .first
            .toSeq
        )
        .toMap
    )

    // capture the logic in a method
    def replaceNullValuesWithAverage(
        df: DataFrame,
        cnames: Array[String]
    ): DataFrame =
      df.na.fill(
        cnames
          .zip(
            df.select(
                df.select(cnames.head, cnames.tail: _*).columns.map(mean): _*
              )
              .first
              .toSeq
          )
          .toMap
      )

    // apply function and check
    replaceNullValuesWithAverage(
      df,
      Array("horsepower", "width", "highway_miles_per_Gallon")
    ).show

    // correlation between attributes

    carsDFWithSchema.stat.corr("Price", "highway_miles_per_gallon")
    carsDFWithSchema.stat
      .corr("city_miles_per_galloon", "highway_miles_per_gallon")

    // find the numeric columns only
    val carsDFNumColumns = carsDFWithSchema.dtypes
      .filter(x => x._2 == "IntegerType" || x._2 == "DoubleType")
      .map(_._1)

    // replace null values with averages for all numeric columns
    replaceNullValuesWithAverage(carsDFWithSchema, carsDFNumColumns)

  }
}
