/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.KinesisInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql._

object cms {


  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("yarn").setAppName("sparketl")
    val spark = SparkSession.builder.appName("Simple Application").config(conf).getOrCreate()

    import spark.implicits._
    val toDouble= udf[Double,String](_.toDouble)

    val dfg = spark.read.option("header","true").csv("s3://ag-mukund/cms/OP_DTL_GNRL_PGYR2017_P06292018.csv")
    //val dfo = spark.read.option("header","true").csv("s3://ag-mukund/cms/OP_DTL_OWNRSHP_PGYR2017_P06292018.csv")
    //val dfr = spark.read.option("header","true").csv("s3://ag-mukund/cms/OP_DTL_RSRCH_PGYR2017_P06292018.csv")

    dfg.take(1)
    val dfg_2 = dfg.withColumn("amount",toDouble(dfg("Total_Amount_of_Payment_USDollars")))
    dfg_2.createOrReplaceTempView("payments")
    dfg_2.first

    case class Payment(physician_id:String, date_payment: String, record_id:String, payer:String, amount:Double, physician_speciality:String, nature_of_payment:String) extends Serializable

    val ds: Dataset[Payment] = spark.sql("select Physician_Profile_ID as physician_id, Date_of_Payment as date_payment, Record_ID as record_id, Applicable_Manufacturer_or_Applicable_GPO_Making_Payment as payer, amount, Physician_Specialty, Nature_of_Payment_or_Transfer as Nature_of_payment from payments1 where Physician_Profile_ID is not null").as[Payment]


    spark.stop()
    
  }
}
             
