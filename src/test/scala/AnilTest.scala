import java.util.concurrent.{Executors, TimeUnit}

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger
case class StreamData(xdrCol1: String)
/*
flume start example

flume-ng agent -Djava.security.auth.login.config=/home/zettauser/kafkajaas.conf -Xmx1024m -Xms1024m -n tier1 -f example-agent.properties

 */
case class customer_lac_info(id: String, cellac: String,contentid:String)

object AnilTest {


  def createReplaceRedisView(spark:SparkSession,tableName:String,tempViewName:String) = {
    import spark.implicits._
    import com.redislabs.provider.redis._
    println("going to replace view")
    //val redis = spark.sparkContext.fromRedisKeyPattern().getKV().map(kv=>customer_lac_info(kv._1,kv._2)).toDS()
    val redis= spark.sparkContext.fromRedisKeyPattern(tableName).getHash().toDS()
    redis.createOrReplaceTempView(tempViewName)
    //redis.show(10)

    println("view replaced")
  }

  def customer_lac_infoUpdates(spark:SparkSession,tableName:String,tempViewName:String): Unit ={
    createReplaceRedisView(spark:SparkSession,tableName,tempViewName)
  }

  def main(args: Array[String]): Unit = {
    val executor = Executors.newScheduledThreadPool(2)

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local[4]")
      .config(new SparkConf()
        // initial redis host - can be any node in cluster mode
        .set("redis.host", "127.0.0.1")

        // initial redis port
        .set("redis.port", "6379"))
      .getOrCreate()

    import spark.implicits._

    val people = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load().as[String].map(i=>StreamData(i)).createTempView("streamXDR")

    customer_lac_infoUpdates(spark,"test:table5","testView")

    executor.scheduleWithFixedDelay(new Runnable {
      override def run(): Unit = customer_lac_infoUpdates(spark,"test:table5","testView")
    },10,10,TimeUnit.SECONDS)


    val joinresult = spark.sql("select p.xdrCol1,r._2 as cellacAndContenID from streamXDR p inner join testView r on r._1=p.xdrCol1")

    // Start running the query that prints the running counts to the console
    val query = joinresult.writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime("1 seconds"))
      .start()

    query.awaitTermination()
  }
}