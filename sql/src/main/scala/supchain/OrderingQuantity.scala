package supchain
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OrderingQuantity {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SCExamples")
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext
    //  println(sc.version + " , " + spark.version)
    sc.setLogLevel("ERROR")
    val medShipmentsLocation = args(0)
//    val medShipmentsLocation = "D:/dloads/med_shipments.csv"
    val medDF = spark.read
      .option("inferSchema", true)
      .option("header", true)
      .csv(medShipmentsLocation)

    // create columns for the dataset without spaces
    // and special characters
    val colnamesForMedDF = Array(
      "ID",
      "projectcode",
      "PQ#",
      "POBySO#",
      "ASNByDN#",
      "Country",
      "ManagedBy",
      "FulfillVia",
      "VendorINCOTerm",
      "ShipmentMode",
      "PQFirstSenttoClientDate",
      "POSenttoVendorDate",
      "ScheduledDeliveryDate",
      "DeliveredtoClientDate",
      "DeliveryRecordedDate",
      "ProductGroup",
      "SubClassification",
      "Vendor",
      "ItemDescription",
      "MoleculeTestType",
      "Brand",
      "Dosage",
      "DosageForm",
      "UnitofMeasurePerPack",
      "LineItemQuantity",
      "LineItemValue",
      "PackPrice",
      "UnitPrice",
      "ManufacturingSite",
      "FirstLineDesignation",
      "WeightKilograms",
      "FreightCostUSD",
      "LineItemInsuranceUSD"
    )

    // zip column names of medfn with the new names and
    // create the expression renaming old colunn to new column
    val newColumnsList =
      medDF.columns.zip(colnamesForMedDF).map(x => col(x._1).as(x._2))
    val meddfn = medDF.select(newColumnsList: _*)

    // explore the data and establish the total cost
    // for molecules provided by vendor
    meddfn
      .groupBy("moleculetesttype", "vendor")
      .agg(
        sum("lineitemquantity").as("totqty"),
        sum("lineitemvalue").as("totvalue"),
        sum("freightcostusd").as("totshippingcost"),
        sum("lineiteminsuranceusd").as("insurcost")
      )
      .withColumn(
        "totalcost",
        $"totqty" + $"totvalue" + $"totshippingcost" + $"insurcost"
      )
      .show(false)

    // medicine country moilecule demand
    val mcmoldemand = meddfn
      .groupBy("country", "moleculetesttype")
      .agg(
        sum("weightkilograms").as("demand")
      )

    // drop na values
    val mcostsnadrop = meddfn
      .select(
        $"vendor",
        $"country",
        $"moleculetesttype",
        col("weightkilograms").cast("double").as("wkg"),
        col("lineitemvalue").cast("double").as("litemval"),
        col("freightcostusd").cast("double").as("frtcost"),
        col("lineiteminsuranceusd").cast("double").as("inscost")
      )
      .na
      .drop

    // get the total number of orders supplied by a vendor
    // to a country for a moleculetesttype
    // that is the supply by the vendor and dividing by the number of orders
    // we could take that as the average vendor quantity supplied in one order

    // and take the non item costs - freight and insurance cost as the order cost
    val mcostsdf = mcostsnadrop
      .groupBy("vendor", "country", "moleculetesttype")
      .agg(
        count("*").as("noorders"),
        sum(col("wkg").cast("double")).as("vsupply"),
        sum("litemval").as("itemcost"),
        sum("frtcost").as("tfrtcost"),
        sum("inscost").as("tinscost")
      )
      .withColumn("tordercost", $"tfrtcost" + $"tinscost")
    mcostsdf.show
    // mcosts dataframe with details - orderrcost and batchsize
    // so the ordercost per order would be the sum of freight and insurance cost
    // divided by the number of orders
    val mcostsdfwd = mcostsdf
      .withColumn("batchsize", $"vsupply" / $"noorders")
      .withColumn("ordercost", $"tordercost" / $"noorders")
    // order cost for weightkilograms is equal to freight cost + lineiteminsuranceusd

    // let's say holding cost per kilogram is between 0.1 and 1 USD per kg
    // generate random double
    val genrdbl = (x: Int) => (1 + new scala.util.Random().nextInt(9)) / 10.0
    val udfgrdbl = udf(genrdbl)
    spark.udf.register("ugendbl", genrdbl)

    // add on random holding cost to the dataframe
    val mcostswrhc = mcostsdfwd.withColumn("rhc", udfgrdbl(lit(1)))

    /*
    Inventory - when, what, how much
    Ordering Cost
    Inventory Carrying Cost = Storage Cost + Capital Cost
    EOQ Formula = Sqrt(2SD / H) - where S is the ordering cost, D is demand, H is holding cost
    Total Logistics Cost
    TLC = average ordering cost + average holding cost + purchase cost
    TLC = (D/Q)*s + (q/2)*h + D*c
    D=2000,c=10,q = 500, s=2000,h=1
    TLC = (2000/500)*2000 + (500/2)* 1 + 2000*10 = 8000 + 250 + 20000 = 28250

    TRC = average ordering cost + average holding cost
    TRCq/TRCq* = 1/2(qstar/q + q/qstar) where qstar is the EOQ
     */

    // demand supply data frame
    val dsdf =
      mcmoldemand.join(mcostswrhc, Array("country", "moleculetesttype"))
    // demand supply data frame with order costs
    val dsdfwithocs = dsdf
      .withColumn(
        "eoq",
        lit(2) * sqrt($"demand" * $"ordercost" / ($"rhc" * $"batchsize"))
      )
      .withColumn(
        "tlc",
        ($"demand" / $"eoq") * $"ordercost" + ($"eoq" / lit(
          2
        )) * $"rhc" * $"batchsize" + ($"itemcost" / $"vsupply") * $"demand"
      )
    dsdfwithocs
      .select("demand", "ordercost", "batchsize", "rhc", "eoq", "tlc")
      .show
  }
}
