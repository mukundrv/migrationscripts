Source:
https://mapr.com/blog/etl-pipeline-healthcare-dataset-with-spark-json-mapr-db/

Data 

wget http://download.cms.gov/openpayments/PGYR17_P062918.ZIP
unzip PGYR17_P062918.ZIP

https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-udfs.html
https://blog.cloudera.com/blog/2017/02/working-with-udfs-in-apache-spark/

-------------------------------
curl https://bintray.com/sbt/rpm/rpm | sudo tee /etc/yum.repos.d/bintray-sbt-rpm.repo
sudo yum install sbt -y 
sudo yum install git -y 

sbt new scala/hello-world.g8


sbt compile

sbt package


//spark-submit --packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.3.0 --class "Stream"  ./target/scala-2.11/hello-world_2.11-1.0.jar
spark-submit --class "Main"  /home/ec2-user/astro/astro/target/scala-2.11/astro_2.11-1.0.jar

-------------------------


name := "Simple Project"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.3.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-kinesis-asl_2.11" % "2.3.0"
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.408"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-kinesis" % "1.11.408"
// https://mvnrepository.com/artifact/org.json4s/json4s-jackson
libraryDependencies += "org.json4s" %% "json4s-native" % "3.6.1"

assemblyMergeStrategy in assembly := {
       case PathList("META-INF", xs @ _*) => MergeStrategy.discard
       case x => MergeStrategy.first
   }

   
   
   ---------------------
   
   
   plugins.sbt

resolvers += Resolver.url("bintray-sbt-plugins", url("http://dl.bintray.com/sbt/sbt-plugin-releases")) //(Resolver.ivyStylePatterns)

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")

----------------------------------------------
sbt compile 

----------------------------------



spark-submit --packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.3.0 --class "Main"  target/scala-2.11/simple-project_2.11-1.0.jar

spark-submit --packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.3.0 --class "Stream"  ./target/scala-2.11/hello-world_2.11-1.0.jar
 spark-submit --packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.3.0 --class "CSV"  ./target/scala-2.11/hello-world_2.11-1.0.jar --files db/GeoLite2-City.mmdb



 --------------------------
 
   
aws kinesis put-record --stream-name tlpstream  --partition-key 123 --data testdata
aws kinesis put-record --stream-name tlpstream  --partition-key 1  --data testdata1
aws kinesis put-record --stream-name tlpstream  --partition-key 2  --data testdata2
aws kinesis put-record --stream-name tlpstream  --partition-key 3  --data testdata3
aws kinesis put-record --stream-name tlpstream  --partition-key 4  --data testdata4
aws kinesis put-record --stream-name tlpstream  --partition-key 5  --data testdata5
aws kinesis put-record --stream-name tlpstream  --partition-key 6  --data testdata6
aws kinesis put-record --stream-name tlpstream  --partition-key 7  --data testdata7



-------------------------------------------


SHARD_ITERATOR=$(aws kinesis get-shard-iterator --shard-id shardId-000000000001 --shard-iterator-type TRIM_HORIZON --stream-name tlpstream --query 'ShardIterator')
aws kinesis get-records --shard-iterator $SHARD_ITERATOR

echo $SHARD_ITERATOR | base64 -d



SHARD_ITERATOR=$(aws kinesis get-shard-iterator --shard-id shardId-000000000002 --shard-iterator-type LATEST --stream-name tlpstream --query 'ShardIterator')
aws kinesis get-records --shard-iterator $SHARD_ITERATOR


------------------------------------------