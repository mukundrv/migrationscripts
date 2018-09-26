/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.KinesisInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.json4s._
import org.json4s.native.JsonParser
import java.io.File
import com.maxmind.geoip2._
import java.net.InetAddress
import com.maxmind.db.CHMCache

object CSV {

  implicit val formats = DefaultFormats
  case class IP(ip: String)

  def getCityName(x: String) : String = {

       val database  = new File("GeoLite2-City.mmdb")
        val reader = new DatabaseReader.Builder(database).withCache(new CHMCache()).build();
        val ip =  InetAddress.getByName(x);
        val response = reader.city(ip);

        val country = response.getCountry();
        println(country.getIsoCode());            // 'US'
        println(country.getName());               // 'United States'
        println(country.getNames().get("zh-CN")); // '美国'

        val subdivision = response.getMostSpecificSubdivision();
        println(subdivision.getName());    // 'Minnesota'
        println(subdivision.getIsoCode()); // 'MN'i

        val city = response.getCity();
        println(city.getName()); // 'Minneapolis'

        val postal = response.getPostal();
        println(postal.getCode()); // '55455'

        val location = response.getLocation();
        println(location.getLatitude());  // 44.9733
        println(location.getLongitude()); // -93.2323


  }

  def parseJson(json: String):Unit = {
        //println(json) 
	val x = JsonParser.parse(json).extract[IP]
	//println(x)
	val name = getCityName(x)

        val database  = new File("GeoLite2-City.mmdb")
//        val database =  new File("/home/hadoop/maxmind/db/GeoLite2-City.mmdb")
        val reader = new DatabaseReader.Builder(database).withCache(new CHMCache()).build();
        val ip =  InetAddress.getByName(x);
        val response = reader.city(ip);

        val country = response.getCountry();
        println(country.getIsoCode());            // 'US'
        println(country.getName());               // 'United States'
        println(country.getNames().get("zh-CN")); // '美国'

        val subdivision = response.getMostSpecificSubdivision();
        println(subdivision.getName());    // 'Minnesota'
        println(subdivision.getIsoCode()); // 'MN'i

        val city = response.getCity();
        println(city.getName()); // 'Minneapolis'

        val postal = response.getPostal();
        println(postal.getCode()); // '55455'

        val location = response.getLocation();
        println(location.getLatitude());  // 44.9733
        println(location.getLongitude()); // -93.2323
       
  }
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("yarn").setAppName("Kinesis")
    //conf.set("deploy-mode","client")
    val batchInterval = Seconds(5)
    val ssc = new StreamingContext(conf, batchInterval)
    val  streamName = "tlpstream"
    val endpointUrl = "kinesis.us-west-2.amazonaws.com"
    val regionName = "us-west-2"
    val appName = "mukundtest"
    val kinesisCheckpointInterval = batchInterval
    val numStreams =4
    /*val kinesisStream = (0 until numStreams).map { i =>
      KinesisInputDStream.builder
         .streamingContext(ssc)
         .endpointUrl("kinesis.us-west-2.amazonaws.com")
         .regionName("us-west-2")
         .streamName("tlpstream")
         .initialPositionInStream(InitialPositionInStream.LATEST)
         .checkpointAppName("mukund")
         .checkpointInterval(batchInterval)
         .storageLevel(StorageLevel.MEMORY_AND_DISK_2)
         .build()
    } */

    val kinesisStream =  KinesisInputDStream.builder
         .streamingContext(ssc)
         .endpointUrl("kinesis.us-west-2.amazonaws.com")
         .regionName("us-west-2")
         .streamName("tlpstream")
         .initialPositionInStream(InitialPositionInStream.LATEST)
         .checkpointAppName("mukund")
         .checkpointInterval(batchInterval)
         .storageLevel(StorageLevel.MEMORY_AND_DISK_2)
         .build()

  // Union all the streams
    //val unionStreams = ssc.union(kinesisStream)

    // Convert each line of Array[Byte] to String, and split into words
    //val words = kinesisStream.flatMap(byteArray => new String(byteArray).split(" "))


    //val csv =  unionStreams.map(parseJson)
    //csv.print()
    //
    kinesisStream.foreachRDD(rdd => {   if(!rdd.isEmpty()) {
        println("************************** RDD count is "+rdd.count())
        rdd.collect().foreach(x => parseJson(new String(x)))
      } })
    // Map each word to a (word, 1) tuple so we can reduce by key to count the words
//    val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)

    // Print the first 10 wordCounts
  //  wordCounts.print()

    // Start the streaming context and await termination
    ssc.start()
    ssc.awaitTermination()
  }
}
             
