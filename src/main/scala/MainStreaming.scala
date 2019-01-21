import java.util.concurrent.{Executors, TimeUnit}

import MainStreamingWithKerberos.createRedisPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.{get_json_object, json_tuple}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.first
import redis.clients.jedis.JedisPool

/*
flume start example

flume-ng agent -Djava.security.auth.login.config=/home/zettauser/kafkajaas.conf -Xmx1024m -Xms1024m -n tier1 -f example-agent.properties

 */

object MainStreaming {

  var campaignNWLUpdatesDF=Seq((1,0,1),(0,0,0))


  def createRedisPool(host: String, port: Int): JedisPool = {
    val pc = new GenericObjectPoolConfig()
    pc.setMaxIdle(5)
    pc.setMaxTotal(5)
    new JedisPool(pc, host, port, 10000)
  }
  def createReplaceRedisView(spark:SparkSession,tableName:String,tempViewName:String):DataFrame = {
    import spark.implicits._
    import com.redislabs.provider.redis._
    println("Replace view   ==" + tempViewName)
    //val redis = spark.sparkContext.fromRedisKeyPattern().getKV().map(kv=>customer_lac_info(kv._1,kv._2)).toDS()
/*
    val myrdd = spark.sparkContext.fromRedisKeyPattern("foo",1).getHash()
    print(myrdd.count())
*/

    var redis = spark.sparkContext.fromRedisKeyPattern(tableName,5).getHash().toDS()
    if (tableName contains "customer") {
      redis=spark.sparkContext.fromRedisKeyPattern("customer-*",5).getKV().toDS()
      //redis.show(6)
    }
   // spark.sparkContext.fromRedisKeyPattern()
    import spark.implicits._
    //redis.show(10)
    if (tableName contains "campaign") {
      val modifiedCampaign = redis.select("_2").as[String]
        .withColumn("_tmp", split($"_2", "\\,"))
        .withColumn("CampaignID", $"_tmp".getItem(0))
        .withColumn("Date1", $"_tmp".getItem(1))
        .withColumn("TimeDif", $"_tmp".getItem(2))
        .withColumn("CELLAC", $"_tmp".getItem(3))
        .withColumn("EndAge", $"_tmp".getItem(4))
        .withColumn("StartAge", $"_tmp".getItem(5))
        .withColumn("CustomerSegments", $"_tmp".getItem(6))
        .withColumn("GenderType", $"_tmp".getItem(7))
        .withColumn("DeviceTypes", $"_tmp".getItem(8))
        .withColumn("CustomerType", $"_tmp".getItem(9))
        .withColumn("Mvno", $"_tmp".getItem(10))
        .withColumn("LineType", $"_tmp".getItem(11))
        .withColumn("profesyonel", $"_tmp".getItem(12))
        .withColumn("ev_hanimi", $"_tmp".getItem(13))
        .withColumn("kamu", $"_tmp".getItem(14))
        .withColumn("prime", $"_tmp".getItem(15))
        .withColumn("woops", $"_tmp".getItem(16))
        .withColumn("diger", $"_tmp".getItem(17))
        .withColumn("BLACKLIST_ID", $"_tmp".getItem(22))
        .withColumn("whitelistID", $"_tmp".getItem(18))
        .withColumn("BLACKLIST_ENABLED", $"_tmp".getItem(23))

        .drop("_tmp")
        .drop("_2")
      //modifiedCampaign.createOrReplaceTempView(tempViewName)
      //modifiedCampaign.show(10)
      return modifiedCampaign
    }
    if (tableName contains "customer") {
      val sqlm="(select cast (msisdn as VARCHAR(100))  as  msisdn ,cast(sex as VARCHAR(100)) as sex ,cast(segment as VARCHAR(100)) as segment ,cast(age as VARCHAR(100)) AS age,cast(  device_type as varchar(100)) as device_type, cast( in_ivt as varchar(100)) as in_ivt, cast( customer_type as varchar(100)) as customer_type,cast( profesyonel as varchar(100)) as profesyonel ,cast( ev_hanimi as varchar(100)) as ev_hanimi,cast( kamu as varchar(100)) as kamu,cast( prime as varchar(100)) as prime,cast( woops as varchar(100)) as woops ,cast( diger as varchar(100)) as diger   ,cast(mvno  as varchar(100)) as mvno ,cast( line_type  as varchar(100)) as line_type,cast( tariff_id as varchar(100)) as tariff_id ,'2-15' as tempBlackList  from customer_lbp) tmp"

/*
      val redis =spark.read.format("jdbc")
        .option("url", "jdbc:oracle:thin:BIGA_CLONE/biga123@//10.248.68.139:1603/BIGAD")
        .option("driver", "oracle.jdbc.driver.OracleDriver")
        .option("dbtable", sqlm).load()
        */

      val modifiedCustomer = redis.select("_2").as[String]
        .withColumn("_tmp", split($"_2", "\\,"))
        .withColumn("msisdn", $"_tmp".getItem(0))
        .withColumn("sex", $"_tmp".getItem(1))
        .withColumn("segment", $"_tmp".getItem(2))
        .withColumn("age", $"_tmp".getItem(3))
        .withColumn("device_type", $"_tmp".getItem(4))
        .withColumn("in_ivt", $"_tmp".getItem(5))
        .withColumn("customer_type", $"_tmp".getItem(6))
        .withColumn("profesyonel", $"_tmp".getItem(7))
        .withColumn("ev_hanimi", $"_tmp".getItem(8))
        .withColumn("kamu", $"_tmp".getItem(9))
        .withColumn("prime", $"_tmp".getItem(10))
        .withColumn("woops", $"_tmp".getItem(11))
        .withColumn("diger", $"_tmp".getItem(12))
        .withColumn("mvno", $"_tmp".getItem(13))
        .withColumn("line_type", $"_tmp".getItem(14))
        .withColumn("tariff_id", $"_tmp".getItem(15))
        .drop("_tmp")
        .drop("_2")
      //modifiedCustomer.createOrReplaceTempView(tempViewName)


      modifiedCustomer.show(10)
      return modifiedCustomer
    }
    if (tableName contains "blacklist") {
      val modifiedBlacklist = redis.select("_2").as[String]
        .withColumn("_tmp", split($"_2", "\\,"))
        .withColumn("Msisdn", $"_tmp".getItem(0))
        .withColumn("Blacklistid", $"_tmp".getItem(1))
        .drop("_tmp")
        .drop("_2")
      //modifiedBlacklist.createOrReplaceTempView(tempViewName)
      //modifiedBlacklist.show(10)
      return modifiedBlacklist
    }

    if (tableName contains "whitelist") {
      val modifiedBlacklist = redis.select("_2").as[String]
        .withColumn("_tmp", split($"_2", "\\,"))
        .withColumn("Msisdn", $"_tmp".getItem(1))
        .withColumn("WLCampaignID", $"_tmp".getItem(0))
        .drop("_tmp")
        .drop("_2")
      //modifiedBlacklist.createOrReplaceTempView(tempViewName)
      //modifiedBlacklist.show(10)
      return modifiedBlacklist
    }
    if (tableName contains "customerblacklist") {
      val modifiedBlacklist = redis.select("_2").as[String]
        .withColumn("_tmp", split($"_2", "\\,"))
        .withColumn("Msisdn", $"_tmp".getItem(0))

        .drop("_tmp")
        .drop("_2")
      //modifiedBlacklist.createOrReplaceTempView(tempViewName)
      //modifiedBlacklist.show(10)
      return modifiedBlacklist
    }
    val emptyDF = redis.select("_2").as[String]
        .withColumn("_tmp", split($"_2", "\\,"))
        .withColumn("Empty", $"_tmp".getItem(0))

        .drop("_tmp")
        .drop("_2")
      return emptyDF
  }
  def customerUpdates(spark:SparkSession,tableName:String,tempViewName:String): DataFrame ={
    return createReplaceRedisView(spark:SparkSession,tableName,tempViewName)
  }
  def campaignNoneWLUpdates(spark:SparkSession,tableName:String,tempViewName:String): DataFrame ={
    return createReplaceRedisView(spark:SparkSession,tableName,tempViewName)
  }
  def campaignWLUpdates(spark:SparkSession,tableName:String,tempViewName:String): DataFrame ={
    return createReplaceRedisView(spark:SparkSession,tableName,tempViewName)
  }
  def blacklistUpdates(spark:SparkSession,tableName:String,tempViewName:String): DataFrame ={
    return createReplaceRedisView(spark:SparkSession,tableName,tempViewName)
  }
  def blacklistTariffUpdates(spark:SparkSession,tableName:String,tempViewName:String): DataFrame ={
    return createReplaceRedisView(spark:SparkSession,tableName,tempViewName)
  }
  def blacklistTariffMvnoUpdates(spark:SparkSession,tableName:String,tempViewName:String): DataFrame ={
    return createReplaceRedisView(spark:SparkSession,tableName,tempViewName)
  }
  def customerblacklistUpdates(spark:SparkSession,tableName:String,tempViewName:String): DataFrame ={
    return createReplaceRedisView(spark:SparkSession,tableName,tempViewName)
  }
  def whitelistUpdates(spark:SparkSession,tableName:String,tempViewName:String): DataFrame ={
    return createReplaceRedisView(spark:SparkSession,tableName,tempViewName)
  }


  def main(args: Array[String]): Unit = {
    val executor = Executors.newScheduledThreadPool(20)

    val spark = SparkSession
      .builder
      .appName("MainStreaming")
      .master("local[*]")
      .config(new SparkConf()
        // initial redis host - can be any node in cluster mode
        //.set("redis.host", "10.248.68.111")
        .set("redis.host", "127.0.0.1")
        // initial redis port
        .set("redis.port", "6379")
        )
      .getOrCreate()
    //dateCol: String,callNumber: String,calledNumber: String,cell:String,lac:String
    import spark.implicits._
    val userSchema = new StructType()
      .add("dateCol", "string")
      .add("callNumber", "string")
      .add("cell", "string")
      .add("lac","string")
/*
    val people = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .option("sep", ",")
      .schema(userSchema)
      .load().createTempView("streamXDR")
*/

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.34.216:9092")
      .option("subscribe", "test_topigim")
      .load()
    //df.show();
    import spark.implicits._
    val comingXDR = df.select("value").as[String].withColumn("_tmp", split($"value", "\\,")).withColumn("Date1", $"_tmp".getItem(0)).withColumn("CallNumber", $"_tmp".getItem(1)).withColumn("CalledNumber", $"_tmp".getItem(2)).withColumn("CELLAC", $"_tmp".getItem(3)).withColumn("isPremature", $"_tmp".getItem(4)).drop("value").drop("_tmp")

    comingXDR.createOrReplaceTempView("streamXDR")
    //spark.catalog.refreshTable("streamXDR")

    campaignNoneWLUpdates(spark,"campaignNoneWL","campaignNoneWLView").cache.createOrReplaceTempView("campaignNoneWLView")
    spark.catalog.refreshTable("campaignNoneWLView")

    campaignWLUpdates(spark,"campaignWL","campaignWLView").cache.createOrReplaceTempView("campaignWLView")
    spark.catalog.refreshTable("campaignWLView")

    customerUpdates(spark,"customerTesttim","customerView").createOrReplaceTempView("customerView")
    spark.catalog.refreshTable("customerView")

    blacklistUpdates(spark,"blacklistTest","blacklistView").cache().createOrReplaceTempView("blacklistView")
    spark.catalog.refreshTable("blacklistView")

    whitelistUpdates(spark,"whitelistTest","whitelistView").cache().createOrReplaceTempView("whitelistView")
    spark.catalog.refreshTable("whitelistView")

    customerblacklistUpdates(spark,"cusblacklist","cusBlackView").cache().createOrReplaceTempView("cusBlackView")
    spark.catalog.refreshTable("cusBlackView")


    executor.scheduleAtFixedRate(new Runnable {
      override def run() {
        println("campaignWLUpdates is running..")
        campaignWLUpdates(spark,"campaignNoneWL","campaignNoneWLView").cache.createOrReplaceTempView("campaignWLView")
        spark.catalog.refreshTable("campaignNoneWLView")
      }
    },5,1,TimeUnit.MINUTES)


    executor.scheduleAtFixedRate(new Runnable {
      override def run() {
        println("campaignWLUpdates is running..")
        campaignWLUpdates(spark,"campaignWL","campaignWLView").cache.createOrReplaceTempView("campaignWLView")
        spark.catalog.refreshTable("campaignWLView")
      }
    },5,1,TimeUnit.MINUTES)
    executor.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        println("customerUpdates is running..")
        customerUpdates(spark,"customerTestt","customerView").cache().createOrReplaceTempView("customerView")
        spark.catalog.refreshTable("customerView")
      }
    },5,1,TimeUnit.MINUTES)
    executor.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        println("whitelistUpdates is running..")
        whitelistUpdates(spark,"whitelistTest","whitelistView").cache().createOrReplaceTempView("whitelistView")
        spark.catalog.refreshTable("blacklistView")
      }
    },5,1,TimeUnit.MINUTES)
    executor.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        println("customerblacklistUpdates is running...")
        customerblacklistUpdates(spark,"cusblacklist","cusBlackView").cache().createOrReplaceTempView("cusBlackView")
        spark.catalog.refreshTable("cusBlackView")
      }
    },5,1,TimeUnit.MINUTES)

    //nonwhitelist

    val campaignControl = spark.sql("select s.CallNumber ,c.CampaignID,c.StartAge,c.EndAge,c.GenderType,c.DeviceTypes,c.CustomerType,c.profesyonel,c.ev_hanimi,c.kamu,c.prime,c.woops,c.diger,c.Mvno,c.LineType,c.CustomerSegments,c.BLACKLIST_ID,s.isPremature,c.CustomerSegments from streamXDR s inner join campaignNoneWLView c on s.CELLAC=c.CELLAC")
    campaignControl.createOrReplaceTempView("campaignJoin")

    val customerControl = spark.sql("select s.CallNumber,s.CampaignID,s.StartAge,s.EndAge,s.GenderType,s.DeviceTypes,s.CustomerType,s.profesyonel,s.ev_hanimi,s.kamu,s.prime,s.woops,s.diger,s.Mvno,s.LineType ,cu.age ,cu.device_type ,cu.tariff_id,s.isPremature ,s.BLACKLIST_ID,s.CustomerSegments from campaignJoin s inner join customerView cu on s.CallNumber=cu.msisdn  and s.StartAge< cu.age and s.EndAge>cu.age and s.CustomerType like cu.customer_type and s.CustomerSegments like cu.segment  and s.GenderType like  cu.sex and s.DeviceTypes like  cu.device_type  and (s.profesyonel=cu.profesyonel or s.ev_hanimi =cu.ev_hanimi or s.kamu=cu.kamu or s.prime=cu.prime or s.woops=cu.woops or s.diger=cu.diger)")
    customerControl.createOrReplaceTempView("customerJoin")

    val blacklistControl = spark.sql("SELECT cu.CallNumber,cu.CampaignID,cu.StartAge,cu.tariff_id,cu.Mvno,cu.isPremature FROM customerJoin cu where cu.CallNumber not in (select bl.Msisdn from blacklistView bl where bl.blacklistid = cu.BLACKLIST_ID)")
    blacklistControl.createOrReplaceTempView("cusCamBlaJoined")

    val cusBlackView_Control = spark.sql("select  cbj.CallNumber,cbj.CampaignID,cbj.tariff_id,cbj.Mvno from cusCamBlaJoined cbj where cbj.CallNumber  not in (select Msisdn from cusBlackView cblk where cblk.Blacklistid =cbj.isPremature )")
    cusBlackView_Control.createOrReplaceTempView("cusBlackView_Controlsg")

    //nonwhitelist
    //whitelist

    val campaignWLControl = spark.sql("select s.CallNumber ,c.CampaignID,c.StartAge,c.EndAge,c.GenderType,c.DeviceTypes,c.CustomerType,c.profesyonel,c.ev_hanimi,c.kamu,c.prime,c.woops,c.diger,c.Mvno,c.LineType,c.CustomerSegments,c.BLACKLIST_ID,s.isPremature,c.CustomerSegments from streamXDR s inner join campaignWLView c on s.CELLAC=c.CELLAC")
    campaignControl.createOrReplaceTempView("wlcampaignCusJoin")

    val whiteListControl = spark.sql("select c.CallNumber ,c.CampaignID,c.StartAge,c.EndAge,c.GenderType,c.DeviceTypes,c.CustomerType,c.profesyonel,c.ev_hanimi,c.kamu,c.prime,c.woops,c.diger,c.Mvno,c.LineType,c.CustomerSegments,c.BLACKLIST_ID,c.isPremature,c.CustomerSegments from wlcampaignCusJoin c inner join whitelistView wl on  c.CallNumber=wl.Msisdn and c.CampaignID = wl.WLCampaignID")
    whiteListControl.createOrReplaceTempView("wlJoin")

    val wlcustomerControl = spark.sql("select s.CallNumber,s.CampaignID,s.StartAge,s.EndAge,s.GenderType,s.DeviceTypes,s.CustomerType,s.profesyonel,s.ev_hanimi,s.kamu,s.prime,s.woops,s.diger,s.Mvno,s.LineType ,cu.age ,cu.device_type ,cu.tariff_id,s.isPremature ,s.BLACKLIST_ID,s.CustomerSegments from wlJoin s inner join customerView cu on s.CallNumber=cu.msisdn  and s.StartAge< cu.age and s.EndAge>cu.age and s.CustomerType like cu.customer_type and s.CustomerSegments like cu.segment  and s.GenderType like  cu.sex and s.DeviceTypes like  cu.device_type  and (s.profesyonel=cu.profesyonel or s.ev_hanimi =cu.ev_hanimi or s.kamu=cu.kamu or s.prime=cu.prime or s.woops=cu.woops or s.diger=cu.diger)")
    wlcustomerControl.createOrReplaceTempView("wlcustomerJoin")

    val wlblacklistControl = spark.sql("SELECT cu.CallNumber,cu.CampaignID,cu.StartAge,cu.tariff_id,cu.Mvno,cu.isPremature FROM wlcustomerJoin cu where cu.CallNumber not in (select bl.Msisdn from blacklistView bl where bl.blacklistid = cu.BLACKLIST_ID)")
    wlblacklistControl.createOrReplaceTempView("wlcusCamBlaJoined")

    val wlcusBlackView_Control = spark.sql("select  cbj.CallNumber,cbj.CampaignID,cbj.tariff_id,cbj.Mvno from wlcusCamBlaJoined cbj where cbj.CallNumber  not in (select Msisdn from cusBlackView cblk where cblk.Blacklistid =cbj.isPremature )")
    wlcusBlackView_Control.createOrReplaceTempView("wlcusBlackView_Controlsg")

    //whitelist
    cusBlackView_Control.unionAll(wlcusBlackView_Control).toDF()

    val query = customerControl.writeStream
              .outputMode("append")
              .format("console")
              .trigger(Trigger.ProcessingTime("1 seconds"))
              .start()


    //cusBlackView_Control.cache()
/*
    val query = cusBlackView_Control.select(to_json(struct("CallNumber","CampaignID","tariff_id","mvno")).alias("value"))
      .writeStream
      .format("kafka")
      .option("checkpointLocation", "/home/kahin/TTDev/check/ff/fff/asf")
      .option("kafka.bootstrap.servers", "192.168.34.216:9092")
      .option("topic", "resultTopic")
      .start()
*/

    query.awaitTermination()
  }
}