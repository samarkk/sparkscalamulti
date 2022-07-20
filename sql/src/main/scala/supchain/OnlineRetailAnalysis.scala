package supchain
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
object OnlineRetailAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SCExamples")
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext
    //  println(sc.version + " , " + spark.version)
    sc.setLogLevel("ERROR")
    val onlineDFPath = args(0)

    //    val onlineDFPath = "D:/dloads/online_retail2.csv"
    val odf = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(onlineDFPath)
    odf.show
    odf.printSchema

    val odfWODups = odf.dropDuplicates
    // check the null values
    odfWODups.count
    odf
      .select($"Description", $"Description".isNull.cast("int").as("descnull"))
      .show
    odf
      .select($"Description", $"Description".isNull.cast("int").as("descnull"))
      .filter("descnull = 1")
      .show
    odf
      .select($"Description", $"Description".isNull.cast("int").as("descnull"))
      .filter("descnull = 1")
      .count

    // add on the year month day of month columns
    val odfwymd = odf
      .withColumn("year", year(to_date($"invoicedate")))
      .withColumn("month", month(to_date($"invoicedate")))
      .withColumn("daymnth", dayofmonth(to_date($"invoicedate")))

    // grouping operations
    // what is the total number of quantities sold by country
    odfwymd
      .groupBy("country")
      .agg(sum($"quantity" * $"price").as("totalval"))
      .show
    // what is the average order size in a country
    odfwymd.groupBy("country").mean("price", "quantity").show
    // what is the average order value, size by country by description
    odfwymd.groupBy("country", "description").mean("price", "quantity").show
    // what are the number of countries and products present
    // countries - 43, countries and description - 30697
    odfwymd.select("country", "description").distinct.count
    // pvioting and unpivoting operations
    odfwymd
      .groupBy("country")
      .pivot("year", Array(2009, 2010))
      .agg(sum("quantity").as("totquant"))
      .show()

    // this will have zero rows as sweden and france are mostly null
    odfwymd
      .groupBy("invoicedate")
      .pivot("country", Seq("United Kingdom", "Sweden", "France"))
      .agg(sum("quantity").as("tquant"))
      .na
      .drop
      .show

    // not dropping the null values and checking
    odfwymd
      .groupBy("invoicedate")
      .pivot("country", Seq("United Kingdom", "Sweden", "France"))
      .agg(sum("quantity").as("tquant"))
      .show

    val pivoteddf = odfwymd
      .groupBy("invoicedate")
      .pivot("country", Seq("United Kingdom", "Sweden", "France"))
      .agg(sum("quantity"))

    // unpivoting, melting
    pivoteddf.select($"invoicedate", expr("stack(1,'Swed',Sweden)")).show
    pivoteddf
      .select(
        $"invoicedate",
        expr(
          "stack(2,'Sweden',Sweden, 'UK', `United Kingdom`) as (country,quantity)"
        )
      )
      .show
    // aggregate by sales and revenue to prepare data for ABC analysis
    val abcDF = odfwymd
      .groupBy("description")
      .agg(
        sum($"quantity").as("total_sales"),
        sum($"quantity" * $"price").as("total_revenue")
      )

    val totrev =
      abcDF.select(sum("total_revenue")).first.toSeq.head.toString.toDouble

    val cumRevSpec =
      sum($"total_revenue") over (Window.orderBy($"total_revenue"))

    abcDF
      .select(
        $"description",
        $"total_sales".alias("sales"),
        $"total_revenue".alias("trev"),
        (cumRevSpec).alias("cumrevenue")
      )
      .show(100, false)

    val abcDFWithCumRev = abcDF.select(
      $"description",
      $"total_sales".alias("sales"),
      $"total_revenue".alias("trev"),
      (cumRevSpec).alias("cumrevenue")
    )
    abcDFWithCumRev.show(100, false)

    // add on a ABC category column
    abcDFWithCumRev
      .selectExpr(
        "description",
        "sales",
        "trev",
        "cumrevenue",
        s"""case when cumrevenue/$totrev < 0.75 then 'A' 
when cumrevenue/$totrev >= 0.75 and cumrevenue/$totrev <= 0.95 then 'B'
else 'C'
end as category"""
      )
      .show(100, false)

    // create the data frame with the ABC category column
    val abcDFWithABCCats = abcDFWithCumRev.selectExpr(
      "description",
      "sales",
      "trev",
      "cumrevenue",
      s"""case when cumrevenue/$totrev < 0.75 then 'A' 
when cumrevenue/$totrev >= 0.75 and cumrevenue/$totrev <= 0.95 then 'B'
else 'C'
end as category"""
    )

    // check the data frame
    abcDFWithABCCats.groupBy("category").agg(sum("trev").as("catrev")).show

    // check the numbers and revenue contribution of products in the ABC analysis
    abcDFWithABCCats
      .groupBy("category")
      .agg(
        count("*").as("catcount"),
        (sum("trev") / lit(totrev)).as("catrevper")
      )
      .show

    // check the item number percentages and their revenue contributions
    val abccount = abcDF.count
    abcDFWithABCCats
      .groupBy("category")
      .agg(
        count("category").as("catcount"),
        (sum("trev") / lit(totrev)).as("catrevper")
      )
      .selectExpr(
        "category",
        "catcount",
        s"catcount/${abccount}*100 as countper",
        "catrevper*100 as revenueper"
      )
      .show

    // carry out the same analysis using sql
    abcDF.createOrReplaceTempView("abctbl")
    spark.sql(s"""
select description, sales,trev, cumrevenue,
case when cumrevenue < 0.75 then 'A' 
when cumrevenue >= 0.75 and cumrevenue <= 0.95 then 'B'
else 'C'
end as category from
(
select description, total_sales as sales, total_revenue as trev,sum(total_revenue) over 
(order by total_revenue)/$totrev as cumrevenue
from abctbl
) f
""").show(100, false)

    // capture the logic into a function

    def abcAnalysis(
        df: DataFrame,
        basiscol: String,
        abccolname: String,
        catcolname: String,
        abclevels: Array[Double]
    ): DataFrame = {
      val totrev = df.select(sum(basiscol)).first.toSeq(0).toString.toDouble
      val cumRevSpecCol = sum(col(basiscol)).over(Window.orderBy(col(basiscol)))
      val ndf = df.withColumn(abccolname, cumRevSpecCol)
      ndf.selectExpr(
        "*",
        s"${abccolname}*100/${totrev} as cumper",
        s"""case when ${abccolname}/$totrev < ${abclevels(0)} then 'A' 
when ${abccolname}/$totrev >= ${abclevels(
          0
        )} and ${abccolname}/$totrev <= ${abclevels(1)} then 'B'
else 'C'
end as ${catcolname}"""
      )
    }

    abcAnalysis(
      abcDF,
      "total_revenue",
      "cumrevenue",
      "categoryrev",
      Array(0.75, 0.95)
    ).show

    abcAnalysis(
      abcDF,
      "total_sales",
      "cumsales",
      "categorysales",
      Array(0.75, 0.95)
    ).show

    // window functions - partition by country
    val abcCountryDF = odfwymd
      .groupBy("country", "description")
      .agg(
        sum($"quantity").as("total_sales"),
        sum($"quantity" * $"price").as("total_revenue")
      )

    abcCountryDF
      .select(
        $"country",
        $"description",
        $"total_sales",
        $"total_revenue",
        sum($"total_revenue")
          .over(
            Window
              .partitionBy($"country")
              .orderBy("total_revenue")
          )
          .alias("cumrevenue")
      )
      .show(100, false)

    // data warehousing functions
    odfwymd
      .cube("country", "year", "month")
      .agg(
        sum("quantity").as("sales"),
        sum($"quantity" * $"price").as("rev")
      )
      .show

    odfwymd
      .rollup("country", "year", "month")
      .agg(
        sum("quantity").as("sales"),
        sum($"quantity" * $"price").as("rev")
      )
      .show
    // grouping sets
    odfwymd.createOrReplaceTempView("odft")
    spark.sql("""
        |select country, year, month, 
        |sum(quantity) as sales,
        |sum(quantity * price) as revenue
        |from odft
        |group by country, year, month
        |grouping sets ((country, year), (country, month))
        |""".stripMargin).show
  }
}
