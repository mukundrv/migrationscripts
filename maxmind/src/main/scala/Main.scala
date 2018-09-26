import java.io.File
import com.maxmind.geoip2._
import java.net.InetAddress
import com.maxmind.db.CHMCache


object Main {
  def main(args: Array[String]) { 
    
    val database =  new File("/home/hadoop/migrationscripts/maxmind/db/GeoLite2-City.mmdb")
    val reader = new DatabaseReader.Builder(database).withCache(new CHMCache()).build();
    val ip =  InetAddress.getByName("128.101.101.101");
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
    //println(ip)
}

}
