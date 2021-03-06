import java.io.IOException
import java.util.concurrent.{Executors, TimeUnit}

import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import com.redislabs.provider.redis._
import com.sun.xml.internal.ws.api.model.ExceptionType
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.split

import scala.util.parsing.json.JSON

class CC[T] { def unapply(a:Any):Option[T] = Some(a.asInstanceOf[T]) }
object M extends CC[Map[String, Any]]

object OracleToRedisJob {

  System.setProperty("java.security.auth.login.config", "/***/***/kafkajaas.conf")


  def mainJsonParser(jsonStr:String,spark:SparkSession): String ={
  //println( "mainJsonParser Called !")
    var EndAge, StartAge, CustomerSegments,GenderType,DeviceTypes,EndDate,StartDate,CustomerType,Mvno,LineType,LifestyleTypes: String = "DEFAULT";
    var profesyonel,ev_hanimi,kamu,prime,woops,diger: String = "-1";
    var CustomerListId :String="0"
    println("Came")
    //val jsonStr="[{\"ParameterType\":\"LOCATION\",\"LocationType\":\"Turkiye\"},{\"ParameterType\":\"AGE_RANGE\",\"EndAge\":65,\"StartAge\":18},{\"ParameterType\":\"CUSTOMER_SEGMENT\",\"CustomerSegments\":[\"A\",\"B\",\"C1\",\"C2\",\"D\",\"E\"]},{\"GenderType\":\"B\",\"ParameterType\":\"CUSTOMER_GENDER_TYPE\"},{\"ParameterType\":\"DEVICE_TYPE\",\"DeviceTypes\":[\"Win\",\"Sym\",\"And\",\"iOS\",\"Bla\",\"Dig\"]},{\"LineType\":\"B\",\"ParameterType\":\"LINE_TYPE\"},{\"ParameterType\":\"MVNO_TYPE\"},{\"EndDate\":\"29.08.2018\",\"StartDate\":\"01.03.2018\",\"ParameterType\":\"DATE_RANGE\"},{\"ParameterType\":\"LIFESTYLE\",\"LifestyleTypes\":[\"woops\",\"prime\",\"profesyonel\",\"kamu\",\"ev_hanimi\",\"diger\"]},{\"CustomerType\":[\"BIREYSEL\"],\"ParameterType\":\"CUSTOMER_TYPE\"}]"
   try {
     val parsed = JSON.parseFull(jsonStr)
     val s4List = parsed.head.asInstanceOf[List[Any]]

     val s4MapList = s4List.map(m => m.asInstanceOf[Map[Any, Any]])

     for (a <- 1 to 10) {
       try {
          EndAge = s4MapList(a)("EndAge").toString.split(".0").head
       }
       catch {
         case e: Exception =>

       }
       try {
          StartAge = s4MapList(a)("StartAge").toString.split(".0").head
       } catch {
         case e: Exception =>

       }
       try {
          CustomerSegments = s4MapList(a)("CustomerSegments").toString.replace("A","1").replace("B","2").replace("C1","3").replace("C2","4").replace("D","5").replace("E","6").replace("List","").replace(",","-").replace(" ","")
       }
       catch {
         case e: Exception =>

       }
       try {
          GenderType = s4MapList(a)("GenderType").toString.replace("F","1").replace("M","2").replace("B","1-2").replace("List","").replace(",","-")
       }
       catch {
         case e: Exception =>

       }
       try {
          DeviceTypes = s4MapList(a)("DeviceTypes").toString.replace("And","4").replace("iOS","1").replace("Win","3").replace("Sym","5").replace("Bla","6").replace("Dig","2").replace("List","").replace(",","-").replace(" ","")
       }
       catch {
         case e: Exception =>

       }
       try {
          CustomerType = s4MapList(a)("CustomerType").toString.replace("BIREYSEL","3").replace("KURUMSAL","1").replace("KOBI","2").replace("List","").replace(",","-")
       }
       catch {
         case e: Exception =>

       }
       try {
          LifestyleTypes = s4MapList(a)("LifestyleTypes").toString.replace("profesyonel","3").replace("kamu","4").replace("woops","1").replace("prime","2").replace("ev_hanimi","5").replace("diger","6").replace("List","").replace(",","-").replace(" ","")
          if (LifestyleTypes.contains("3")) {
            profesyonel = "1"
          }
          if (LifestyleTypes.contains("4")) {
            kamu = "1"
          }
          if (LifestyleTypes.contains("1")) {
            woops = "1"
          }
          if (LifestyleTypes.contains("2")) {
            prime = "1"
          }
          if (LifestyleTypes.contains("5")) {
            ev_hanimi = "1"
          }
          if (LifestyleTypes.contains("6")) {
            diger = "1"
          }
       }
       catch {
         case e: Exception =>

       }
       try{
         CustomerListId=s4MapList(a)("CustomerListId").toString.split(".").head
         println(CustomerListId+ " CustomerListId")
       } catch {
         case e: Exception =>

       }
       try {
         Mvno = s4MapList(a)("Mvno").toString.replace("TEKNOSAMOBIL","9").replace("AVEA","8").replace("FENERCELL","7").replace("PTTCELL","6").replace("BIMCELL","5").replace("GSMOBILE","4").replace("POCELL","3").replace("TRABZONCELL","2").replace("KARTALCELL","1").replace("List","").replace(",","-")

       } catch {
         case e: Exception =>

       }
       try {
          LineType = s4MapList(a)("LineType").toString.replace("POSTPAID","1").replace("PREPAID","2").replace("B","1-2")
       } catch {
         case e: Exception =>

       }
     }
   }
   catch {
     case e: Exception =>

   }
    println("returned")
    val parameterString=EndAge +","+StartAge+","+CustomerSegments+","+GenderType+","+DeviceTypes+","+CustomerType+","+Mvno+","+LineType+","+ profesyonel  +","+  ev_hanimi  +","+   kamu +","+ prime   +","+   woops +","+  diger +","+CustomerListId
   return parameterString



  }
  def whiteListJsonParser(jsonStr:String,spark:SparkSession,ttlTime:BigInt): String ={
    //println( "mainJsonParser Called !")
    var profesyonel,CustomerListId,ev_hanimi,kamu,prime,woops,diger: String = "0";
    //val jsonStr="[{\"ParameterType\":\"LOCATION\",\"LocationType\":\"Turkiye\"},{\"ParameterType\":\"AGE_RANGE\",\"EndAge\":65,\"StartAge\":18},{\"ParameterType\":\"CUSTOMER_SEGMENT\",\"CustomerSegments\":[\"A\",\"B\",\"C1\",\"C2\",\"D\",\"E\"]},{\"GenderType\":\"B\",\"ParameterType\":\"CUSTOMER_GENDER_TYPE\"},{\"ParameterType\":\"DEVICE_TYPE\",\"DeviceTypes\":[\"Win\",\"Sym\",\"And\",\"iOS\",\"Bla\",\"Dig\"]},{\"LineType\":\"B\",\"ParameterType\":\"LINE_TYPE\"},{\"ParameterType\":\"MVNO_TYPE\"},{\"EndDate\":\"29.08.2018\",\"StartDate\":\"01.03.2018\",\"ParameterType\":\"DATE_RANGE\"},{\"ParameterType\":\"LIFESTYLE\",\"LifestyleTypes\":[\"woops\",\"prime\",\"profesyonel\",\"kamu\",\"ev_hanimi\",\"diger\"]},{\"CustomerType\":[\"BIREYSEL\"],\"ParameterType\":\"CUSTOMER_TYPE\"}]"
    try {
      val parsed = JSON.parseFull(jsonStr)
      val s4List = parsed.head.asInstanceOf[List[Any]]

      val s4MapList = s4List.map(m => m.asInstanceOf[Map[Any, Any]])

      for (a <- 1 to 10) {
        try{
          CustomerListId=s4MapList(a)("CustomerListId").toString.split(".").head
          println(CustomerListId+ " CustomerListId")
        } catch {
          case e: Exception =>
        }
      }
    }
    catch {
      case e: Exception =>

    }
    import spark.implicits._
    return CustomerListId
  }
  def parameterjsonParser(queryDF:DataFrame,spark:SparkSession,ttlTime:Int): Unit ={
    //queryDF.show()
    print(queryDF.printSchema())

    // session icinde yarattigin bir DF icerisinde dogrudan ilerleyemiyorsun.
    val otherDF=queryDF.collect()
    //degisik bir problemdi.

    var jsonString=""
    val myRdd=queryDF.rdd
      .map(row => {
        var parameter = row.getString(0)
        val accounid = row.getString(1)
        val  QUERYID= row.getString(2)
        //parameter=mainJsonParser(parameter,spark)
        jsonString = accounid + "," + parameter
        (QUERYID.toString, jsonString)
      }).map{ case (mykey, myvalue) => (mykey.toString, myvalue.toString) }
     println(jsonString)
    println(myRdd.take(100))

    redisOperations(myRdd,"queryTable",ttlTime)

  }
  def createDataframeFromOracle(spark:SparkSession,sqlQuery : String,tableName:String,myCase:String,ttlTime:Int): Unit =

{
    /*
    val jdbcDF =spark.read.format("jdbc")
      .option("url", "jdbc:oracle:thin:BIGA_CLONE/biga123@//10.248.68.139:1603/BIGAD")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      //.option("dbtable", "(select REGEXP_SUBSTR(ROW_NUMBER() OVER (ORDER BY cellid, lac), '[^,]+', 1, 1) as column_id,concat(cellid,lac)  as cellac, CAST (content_id AS VARCHAR(100)) as contentid from lac_cell_info) tmp")
      //.option("dbtable", "(select cast(payment_type as VARCHAR(100) , cast(blacklist_id as VARCHAR(100) from campaign) tmp")
      */
    println("Oracle Connection started...")
    val jdbcDF =spark.read.format("jdbc")
      .option("url", "jdbc:oracle:thin:BIGA_CLONE/biga123@//10.248.68.139:1603/BIGAD")
      .option("driver", "oracle.jdbc.driver.OracleDriver").option("dbtable", sqlQuery).load()

    //jdbcDF.na.fill('0').show()
    //jdbcDF.show(10)
  println("Oracle Connection finished...")


    myCase match {
   // case "1" => campaignToRedis(spark,jdbcDF,tableName)
   // case "2" => queryToRedis(spark,jdbcDF,tableName)
    case "3" => customerToRedis(spark,jdbcDF,tableName,ttlTime)
    case "4" => campaignandQueryToRedis(spark,jdbcDF,tableName,ttlTime)
    case "2" => blacklistToRedis(spark,jdbcDF,tableName,ttlTime)
    case "6" => whitelistToRedis(spark,jdbcDF,tableName,ttlTime)
    case "7" => blackListTariffToRedis(spark,jdbcDF,tableName,ttlTime)
    case "8" => blackListMVNOToRedis(spark,jdbcDF,tableName,ttlTime)
    case "9" => customerblacklistToRedis(spark,jdbcDF,tableName,ttlTime)

    case "5" =>println("Test")
    }
 }
  def testts(): Unit ={
    val userSchema = new StructType().add("name", "string").add("age", "string")
    val spark = SparkSession.builder.appName("ConnectingOracleDatabase").master("local").getOrCreate()

    val csvDF = spark.readStream.schema(userSchema).csv("/home/kahin/TTDemo/ff")
    csvDF.createOrReplaceTempView("myTable")

    csvDF.groupBy("name").count().writeStream.outputMode("complete").format("console").start().awaitTermination()
  }
  def OracleConnection(spark:SparkSession): Unit  =
  {



  }
  def redisOperations (myRdd: RDD[(String,String)],redisTableName:String,ttlTime:Int): Unit ={
    import com.redislabs.provider.redis._
    println("Started to saving REDIS for  :  " + redisTableName)
    //val redisDB = ("127.0.0.1", 6379)
    val sc = SparkContext.getOrCreate()

    //myRdd.collect().foreach(println)
    val redisConfig = new RedisConfig(new RedisEndpoint("127.0.0.1", 6379))
    print("Saving rdd to redis table name :  " + redisTableName)
    sc.toRedisHASH(myRdd, redisTableName)(redisConfig)
   // sc.toRedisKV( myRdd)(redisConfig)
  }

  def redisOperationsForCustomer (myRdd: RDD[(String,String)],redisTableName:String,ttlTime:Int): Unit ={
    import com.redislabs.provider.redis._
    println("Started to saving REDIS for  :  " + redisTableName)
    //val redisDB = ("127.0.0.1", 6379)
    val sc = SparkContext.getOrCreate()

    //myRdd.collect().foreach(println)
    val redisConfig = new RedisConfig(new RedisEndpoint("127.0.0.1", 6379))
    print("Saving rdd to redis keys  :  " + redisTableName)
    //sc.toRedisHASH(myRdd, redisTableName)(redisConfig)
    sc.toRedisKV(myRdd)(redisConfig)
  }


  def campaignToRedis(spark:SparkSession,mydataFrame: DataFrame ,redisTableName:String,ttlTime:Int)  {
    print(mydataFrame.printSchema())
    val myRdd=mydataFrame.rdd
      .map(row => {
        val QUERYID = row.getString(0)
        val PAYMENT_TYPE = row.getString(1)
        val BLACKLIST_ID = row.getString(2)
        val jsonString = PAYMENT_TYPE + "," + BLACKLIST_ID

        (QUERYID.toString, jsonString)
      }).map{ case (mykey, myvalue) => (mykey.toString, myvalue.toString) }
    //print(myRdd)
    redisOperations(myRdd,redisTableName,ttlTime)
  }

  def campaignandQueryToRedis(spark:SparkSession,mydataFrame: DataFrame ,redisTableName:String,ttlTime:Int)  {
    print(mydataFrame.printSchema())

    val myRdd=mydataFrame.rdd
      .map(row => {
        val incrementID = row.getString(0)
        val campaignID = row.getString(1)
        val CAMPAIGNENDDATE = row.getString(2)
        val TIME_DIFFERENCE = row.getString(3)
        val  CILAC= row.getString(4)
        var  PARAMETERS= row.getString(5)
        PARAMETERS=mainJsonParser(PARAMETERS,spark)
        val  ACCOUNTID= row.getString(6)
        val  SMSQUOTAUSED= row.getString(7)
        val  SMSQUOTA= row.getString(8)
        val  PAYMENT_TYPE= row.getString(9)
        val  BLACKLIST_ID= row.getString(10)

        val jsonString = campaignID + "," + CAMPAIGNENDDATE +","+ TIME_DIFFERENCE+","+CILAC+","+PARAMETERS+","+ACCOUNTID+","+SMSQUOTAUSED +","+SMSQUOTA+","+ PAYMENT_TYPE+ ","+BLACKLIST_ID

        (incrementID.toString, jsonString)
      }).map{ case (mykey, myvalue) => (mykey.toString, myvalue.toString) }
    //print(myRdd)
    redisOperations(myRdd,redisTableName,ttlTime)
  }



  def queryToRedis(spark:SparkSession,queryDataFrame:DataFrame ,redisTableName:String,ttlTime:Int)  {
    //queryDF.show()
    print(queryDataFrame.printSchema())

    // session icinde yarattigin bir DF icerisinde dogrudan ilerleyemiyorsun.
    //val otherDF=queryDataFrame.collect()
    //degisik bir problemdi.

    var jsonString=""
    val myRdd=queryDataFrame.rdd
      .map(row => {
        var parameter = row.getString(0)
        val accounid = row.getString(1)
        val  QUERYID= row.getString(2)
        parameter=mainJsonParser(parameter,spark)
        jsonString = accounid + "," + parameter
        (QUERYID.toString, jsonString)
      }).map{ case (mykey, myvalue) => (mykey.toString, myvalue.toString) }

    redisOperations(myRdd,redisTableName,ttlTime)
  }


  def blacklistToRedis(spark:SparkSession,queryDataFrame:DataFrame ,redisTableName:String,ttlTime:Int)  {
    //queryDF.show()
    print(queryDataFrame.printSchema())

    // session icinde yarattigin bir DF icerisinde dogrudan ilerleyemiyorsun.
    //val otherDF=queryDataFrame.collect()
    //degisik bir problemdi.

    var jsonString=""
    val myRdd=queryDataFrame.rdd
      .map(row => {
        var msisdn = row.getString(0)
        val blacklistid = row.getString(1)
        val  incrementID= row.getString(2)
        jsonString = msisdn + "," + blacklistid
        (incrementID.toString, jsonString)
      }).map{ case (mykey, myvalue) => (mykey.toString, myvalue.toString) }
    redisOperations(myRdd,redisTableName,ttlTime)
  }


  def whitelistToRedis(spark:SparkSession,queryDataFrame:DataFrame ,redisTableName:String,ttlTime:Int)  {
    //queryDF.show()
    print(queryDataFrame.printSchema())
    var jsonString=""
    val myRdd=queryDataFrame.rdd
      .map(row => {
        var msisdn = row.getString(2)
        val whitelistid = row.getString(1)
        val  incrementID= row.getString(0)
        jsonString = msisdn + "," + whitelistid
        (incrementID.toString, jsonString)
      }).map{ case (mykey, myvalue) => (mykey.toString, myvalue.toString) }
    redisOperations(myRdd,redisTableName,ttlTime)
  }

  def blackListTariffToRedis(spark:SparkSession,queryDataFrame:DataFrame ,redisTableName:String,ttlTime:Int) {
    //queryDF.show()
    print(queryDataFrame.printSchema())

    // session icinde yarattigin bir DF icerisinde dogrudan ilerleyemiyorsun.
    //val otherDF=queryDataFrame.collect()
    //degisik bir problemdi.

    var jsonString = ""
    val myRdd = queryDataFrame.rdd
      .map(row => {
        val tariffID = row.getString(1)
        val incrementID = row.getString(0)
        jsonString = tariffID
        (incrementID.toString, jsonString)
      }).map { case (mykey, myvalue) => (mykey.toString, myvalue.toString) }
    redisOperations(myRdd, redisTableName,ttlTime)
  }
  def blackListMVNOToRedis(spark:SparkSession,queryDataFrame:DataFrame ,redisTableName:String,ttlTime:Int) {
    //queryDF.show()

    print(queryDataFrame.printSchema())

    // session icinde yarattigin bir DF icerisinde dogrudan ilerleyemiyorsun.
    //val otherDF=queryDataFrame.collect()
    //degisik bir problemdi.
    //mvno , tariff_id
    var jsonString = ""
    val myRdd = queryDataFrame.rdd
      .map(row => {
        val tariff_id = row.getString(2)
        val mvno = row.getString(1)
        val incrementID = row.getString(0)
        jsonString = mvno + "," + tariff_id
        (incrementID.toString, jsonString)
      }).map { case (mykey, myvalue) => (mykey.toString, myvalue.toString) }
    redisOperations(myRdd, redisTableName,ttlTime)
  }

  def customerblacklistToRedis(spark:SparkSession,queryDataFrame:DataFrame ,redisTableName:String,ttlTime:Int) {
    //queryDF.show()

    print(queryDataFrame.printSchema())

    // session icinde yarattigin bir DF icerisinde dogrudan ilerleyemiyorsun.
    //val otherDF=queryDataFrame.collect()
    //degisik bir problemdi.
    //mvno , tariff_id
    var jsonString = ""
    val myRdd = queryDataFrame.rdd
      .map(row => {
        val blacklistcatid = row.getString(2)
        val msisdn = row.getString(1)
        val incrementID = row.getString(0)
        jsonString = msisdn +','+blacklistcatid
        (incrementID.toString, jsonString)
      }).map { case (mykey, myvalue) => (mykey.toString, myvalue.toString) }
    redisOperations(myRdd, redisTableName,ttlTime)
  }
    def customerToRedis(spark:SparkSession,customerDataFrame:DataFrame ,redisTableName:String,ttlTime:Int): Unit ={
    //degisik bir problemdi.
    println("In Customer")
    var jsonString=""
    val myRdd=customerDataFrame.rdd
      .map(row => {
        var msisdn = row.getString(0)
        val sex = "%"+row.getString(1)+"%"
        val segment = "%"+row.getString(2)+"%"
        val  age= row.getString(3)
        val  device_type= "%"+row.getString(4)+"%"
        val  in_ivt= "%"+row.getString(5)+"%"
        val  customer_type= "%"+row.getString(6)+"%"
        val  profesyonel= row.getString(7)
        val  ev_hanimi=row.getString(8)
        val  kamu= row.getString(9)
        val  prime= row.getString(10)
        val  woops= row.getString(11)
        val  diger= row.getString(12)
        val  mvno= "%"+row.getString(13)+"%"
        val  line_type= "%"+row.getString(14)+"%"
        val  tariff_id= row.getString(15)
        val  incrementID= "customer-"+row.getString(17)
        val  tempBlackList= row.getString(16)
        jsonString = msisdn+","+sex+","+segment + "," + age+","+device_type+","+in_ivt+","+customer_type+","+profesyonel+","+ev_hanimi+","+
          kamu+ "," +prime+ "," +woops+ "," +diger+ "," +mvno+ "," +line_type+ "," +tariff_id+","+tempBlackList
        (incrementID.toString, jsonString)
      }).map{ case (mykey, myvalue) => (mykey.toString, myvalue.toString) }
      redisOperationsForCustomer(myRdd,redisTableName,ttlTime)
  }

    def dataframeToRDD(spark:SparkSession,mydataFrame: DataFrame ,redisTableName:String) = {
    import spark.implicits._
    import com.redislabs.provider.redis._
    //val redisDB = ("127.0.0.1", 6379)
    val sc = SparkContext.getOrCreate()
    //val mydataFramee = mydataFrame.as[(String,String)]

    print(mydataFrame.printSchema())

    val myRdd=mydataFrame.rdd
      .map(row => {
        val id = row.getString(0)
        val CELLAC = row.getString(1)
        val contentid = row.getString(2)
        val jsonString = CELLAC + "," + contentid

        (id.toString, jsonString)
      }).map{ case (word, count) => (word.toString, count.toString) }
/*
    val myRdd = sc.textFile("/home/kahin/py-faster-rcnn/LICENSE.txt")
      .keyBy(line => line.substring(1,2).trim())
      .mapValues(line => (    line.substring(2,5).trim()
      )
      )
      */
    print(myRdd)
    myRdd.take(10)
    myRdd.collect().foreach(println)

      val redisConfig = new RedisConfig(new RedisEndpoint("127.0.0.1", 6379 ,timeout = 200000))
    sc.toRedisHASH(myRdd, redisTableName)(redisConfig)
  }

  def main(args: Array[String]): Unit = {
    //OracleConnection()
    println("Oracle tables to redis job started!")
    val spark = SparkSession.builder.appName("Cod").master("local").getOrCreate()

    val executor = Executors.newScheduledThreadPool(20)

    println("customerqlQuery running....")
    val customerqlQuery="(select   cast (msisdn as VARCHAR(100))  as  msisdn ,cast(sex as VARCHAR(100)) as sex ,cast(segment as VARCHAR(100)) as segment ,cast(age as VARCHAR(100)) AS age,cast(  device_type as varchar(100)) as device_type, cast( in_ivt as varchar(100)) as in_ivt, cast( customer_type as varchar(100)) as customer_type,cast( profesyonel as varchar(100)) as profesyonel ,cast( ev_hanimi as varchar(100)) as ev_hanimi,cast( kamu as varchar(100)) as kamu,cast( prime as varchar(100)) as prime,cast( woops as varchar(100)) as woops ,cast( diger as varchar(100)) as diger   ,cast(mvno  as varchar(100)) as mvno ,cast( line_type  as varchar(100)) as line_type,cast( tariff_id as varchar(100)) as tariff_id ,'2-15' as tempBlackList, REGEXP_SUBSTR(ROW_NUMBER() OVER (ORDER BY diger), '[^,]+', 1, 1) as increment_id   from customer_lbp ) tmp"
    createDataframeFromOracle(spark,customerqlQuery,"customerTesttim","3",50000) //3


    /*
    println("campaignNoneWLTestQuery running....")
    val campaignNoneWLTestQuery ="(select REGEXP_SUBSTR(ROW_NUMBER() OVER (ORDER BY lc.cellid, lc.lac), '[^,]+', 1, 1) as increment_id ,cast(ca.id as VARCHAR(100)) as id, cast(ca.campaignenddate as  VARCHAR(100)) as campaignenddate, cast( ca.TIME_DIFFERENCE as  VARCHAR(100)) as TIME_DIFFERENCE , lc.cellid || lc.lac as cilac,q.parameters,  cast( q.accountid as VARCHAR(100)) as accountid ,cast(  acc.smsquotaused as VARCHAR(100)) as smsquotaused ,  cast (acc.smsquota as VARCHAR(100)) as  smsquota, cast( ca.payment_type as VARCHAR(100)) AS payment_type, cast( ca.blacklist_id  as VARCHAR(100)) as blacklist_id   FROM campaign ca , content co , lac_cell_info lc,query q ,account acc, working_days wd where  trunc(wd.start_day)<=trunc(sysdate) and trunc(wd.end_day)>=trunc(sysdate) and ca.MSISDNLIST_ID=0 and   wd.CAMPAIGN_ID=ca.id and  ca.id = co.campaign_id and co.id = lc.content_id and ca.queryid = q.queryid and acc.accountid = q.accountid and ca.campaignstatus= 'APPROVED' and q.query_type=4 and  trunc(ca.campaignstartdate) <= trunc(sysdate) and trunc(ca.campaignenddate) >=  trunc(sysdate) and cast( (trunc(wd.start_hour) || '.'|| trunc(wd.start_minute)) as number (5) )<=concat(( sysdate - trunc(sysdate) ) * 24, extract(hour from current_timestamp) ) and cast( (trunc(wd.end_hour) || '.'|| trunc(wd.end_minute)) as number (5) )>=concat(( sysdate - trunc(sysdate) ) * 24, extract(hour from current_timestamp))) tmp"
    //val campaignTestQuery="(select REGEXP_SUBSTR(ROW_NUMBER() OVER (ORDER BY lc.cellid, lc.lac), '[^,]+', 1, 1) as increment_id ,cast(ca.id as VARCHAR(100)) as id, cast(ca.campaignenddate as  VARCHAR(100)) as campaignenddate, cast( ca.TIME_DIFFERENCE as  VARCHAR(100)) as TIME_DIFFERENCE , lc.cellid || lc.lac as cilac,q.parameters,  cast( q.accountid as VARCHAR(100)) as accountid ,cast(  acc.smsquotaused as VARCHAR(100)) as smsquotaused ,  cast (acc.smsquota as VARCHAR(100)) as  smsquota, cast( ca.payment_type as VARCHAR(100)) AS payment_type, cast( ca.blacklist_id  as VARCHAR(100)) as blacklist_id  FROM campaign ca , content co , lac_cell_info lc,query q ,account acc where ca.id = co.campaign_id and co.id = lc.content_id and ca.queryid = q.queryid and acc.accountid = q.accountid and ca.campaignstatus= 'APPROVED' and q.query_type=4 and  trunc(ca.campaignstartdate) <= trunc(sysdate) and trunc(ca.campaignenddate) >=  trunc(sysdate)) tmp"
    createDataframeFromOracle(spark,campaignNoneWLTestQuery,"campaignNoneWL","4",5) //4
*/
    /*
    executor.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        println("campaignNoneWLTestQuery running....")
        val campaignNoneWLTestQuery ="(select REGEXP_SUBSTR(ROW_NUMBER() OVER (ORDER BY lc.cellid, lc.lac), '[^,]+', 1, 1) as increment_id ,cast(ca.id as VARCHAR(100)) as id, cast(ca.campaignenddate as  VARCHAR(100)) as campaignenddate, cast( ca.TIME_DIFFERENCE as  VARCHAR(100)) as TIME_DIFFERENCE , lc.cellid || lc.lac as cilac,q.parameters,  cast( q.accountid as VARCHAR(100)) as accountid ,cast(  acc.smsquotaused as VARCHAR(100)) as smsquotaused ,  cast (acc.smsquota as VARCHAR(100)) as  smsquota, cast( ca.payment_type as VARCHAR(100)) AS payment_type, cast( ca.blacklist_id  as VARCHAR(100)) as blacklist_id   FROM campaign ca , content co , lac_cell_info lc,query q ,account acc, working_days wd where  trunc(wd.start_day)<=trunc(sysdate) and trunc(wd.end_day)>=trunc(sysdate) and ca.MSISDNLIST_ID=0 and   wd.CAMPAIGN_ID=ca.id and  ca.id = co.campaign_id and co.id = lc.content_id and ca.queryid = q.queryid and acc.accountid = q.accountid and ca.campaignstatus= 'APPROVED' and q.query_type=4 and  trunc(ca.campaignstartdate) <= trunc(sysdate) and trunc(ca.campaignenddate) >=  trunc(sysdate) and cast( (trunc(wd.start_hour) || '.'|| trunc(wd.start_minute)) as number (5) )<=concat(( sysdate - trunc(sysdate) ) * 24, extract(hour from current_timestamp) ) and cast( (trunc(wd.end_hour) || '.'|| trunc(wd.end_minute)) as number (5) )>=concat(( sysdate - trunc(sysdate) ) * 24, extract(hour from current_timestamp))) tmp"
        //val campaignTestQuery="(select REGEXP_SUBSTR(ROW_NUMBER() OVER (ORDER BY lc.cellid, lc.lac), '[^,]+', 1, 1) as increment_id ,cast(ca.id as VARCHAR(100)) as id, cast(ca.campaignenddate as  VARCHAR(100)) as campaignenddate, cast( ca.TIME_DIFFERENCE as  VARCHAR(100)) as TIME_DIFFERENCE , lc.cellid || lc.lac as cilac,q.parameters,  cast( q.accountid as VARCHAR(100)) as accountid ,cast(  acc.smsquotaused as VARCHAR(100)) as smsquotaused ,  cast (acc.smsquota as VARCHAR(100)) as  smsquota, cast( ca.payment_type as VARCHAR(100)) AS payment_type, cast( ca.blacklist_id  as VARCHAR(100)) as blacklist_id  FROM campaign ca , content co , lac_cell_info lc,query q ,account acc where ca.id = co.campaign_id and co.id = lc.content_id and ca.queryid = q.queryid and acc.accountid = q.accountid and ca.campaignstatus= 'APPROVED' and q.query_type=4 and  trunc(ca.campaignstartdate) <= trunc(sysdate) and trunc(ca.campaignenddate) >=  trunc(sysdate)) tmp"
        createDataframeFromOracle(spark,campaignNoneWLTestQuery,"campaignNoneWL","4",5) //4
      }
    },0,5,TimeUnit.MINUTES)
    executor.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        println("campaignWLTestQuery running...")
        val campaignWLTestQuery ="(select REGEXP_SUBSTR(ROW_NUMBER() OVER (ORDER BY lc.cellid, lc.lac), '[^,]+', 1, 1) as increment_id ,cast(ca.id as VARCHAR(100)) as id, cast(ca.campaignenddate as  VARCHAR(100)) as campaignenddate, cast( ca.TIME_DIFFERENCE as  VARCHAR(100)) as TIME_DIFFERENCE , lc.cellid || lc.lac as cilac,q.parameters,  cast( q.accountid as VARCHAR(100)) as accountid ,cast(  acc.smsquotaused as VARCHAR(100)) as smsquotaused ,  cast (acc.smsquota as VARCHAR(100)) as  smsquota, cast( ca.payment_type as VARCHAR(100)) AS payment_type, cast( ca.blacklist_id  as VARCHAR(100)) as blacklist_id   FROM campaign ca , content co , lac_cell_info lc,query q ,account acc, working_days wd where ca.MSISDNLIST_ID>0 and  wd.CAMPAIGN_ID=ca.id and  ca.id = co.campaign_id and co.id = lc.content_id and ca.queryid = q.queryid and acc.accountid = q.accountid and ca.campaignstatus= 'APPROVED' and q.query_type=4 and  trunc(ca.campaignstartdate) <= trunc(sysdate) and trunc(ca.campaignenddate) >=  trunc(sysdate) and cast( (trunc(wd.start_hour) || '.'|| trunc(wd.start_minute)) as number (5) )<=concat(( sysdate - trunc(sysdate) ) * 24, extract(hour from current_timestamp) ) and cast( (trunc(wd.end_hour) || '.'|| trunc(wd.end_minute)) as number (5) )>=concat(( sysdate - trunc(sysdate) ) * 24, extract(hour from current_timestamp))) tmp"
        //val campaignTestQuery="(select REGEXP_SUBSTR(ROW_NUMBER() OVER (ORDER BY lc.cellid, lc.lac), '[^,]+', 1, 1) as increment_id ,cast(ca.id as VARCHAR(100)) as id, cast(ca.campaignenddate as  VARCHAR(100)) as campaignenddate, cast( ca.TIME_DIFFERENCE as  VARCHAR(100)) as TIME_DIFFERENCE , lc.cellid || lc.lac as cilac,q.parameters,  cast( q.accountid as VARCHAR(100)) as accountid ,cast(  acc.smsquotaused as VARCHAR(100)) as smsquotaused ,  cast (acc.smsquota as VARCHAR(100)) as  smsquota, cast( ca.payment_type as VARCHAR(100)) AS payment_type, cast( ca.blacklist_id  as VARCHAR(100)) as blacklist_id  FROM campaign ca , content co , lac_cell_info lc,query q ,account acc where ca.id = co.campaign_id and co.id = lc.content_id and ca.queryid = q.queryid and acc.accountid = q.accountid and ca.campaignstatus= 'APPROVED' and q.query_type=4 and  trunc(ca.campaignstartdate) <= trunc(sysdate) and trunc(ca.campaignenddate) >=  trunc(sysdate)) tmp"
        createDataframeFromOracle(spark,campaignWLTestQuery,"campaignWL","4",5) //4
      }
    },5,5,TimeUnit.HOURS)
    executor.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        println("customerqlQuery running....")
        val customerqlQuery="(select   cast (msisdn as VARCHAR(100))  as  msisdn ,cast(sex as VARCHAR(100)) as sex ,cast(segment as VARCHAR(100)) as segment ,cast(age as VARCHAR(100)) AS age,cast(  device_type as varchar(100)) as device_type, cast( in_ivt as varchar(100)) as in_ivt, cast( customer_type as varchar(100)) as customer_type,cast( profesyonel as varchar(100)) as profesyonel ,cast( ev_hanimi as varchar(100)) as ev_hanimi,cast( kamu as varchar(100)) as kamu,cast( prime as varchar(100)) as prime,cast( woops as varchar(100)) as woops ,cast( diger as varchar(100)) as diger   ,cast(mvno  as varchar(100)) as mvno ,cast( line_type  as varchar(100)) as line_type,cast( tariff_id as varchar(100)) as tariff_id ,'2-15' as tempBlackList, REGEXP_SUBSTR(ROW_NUMBER() OVER (ORDER BY diger), '[^,]+', 1, 1) as increment_id   from customer_lbp) tmp"
        createDataframeFromOracle(spark,customerqlQuery,"customerTestt","3",5) //3
      }
    },0,100,TimeUnit.MINUTES)
    executor.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        println("blacklistQuery running...")
        val blacklistQuery="(select cast(msisdn as VARCHAR(100)) as msisdn ,cast(black_list_id as VARCHAR(100)) as blacklistid , REGEXP_SUBSTR(ROW_NUMBER() OVER (ORDER BY msisdn), '[^,]+', 1, 1) as increment_id   from black_list_msisdns) tmp"
        createDataframeFromOracle(spark,blacklistQuery,"blacklistTest","2",5) //2
      }
    },0,4,TimeUnit.HOURS)
    executor.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        println("whitelistQuery running...")
        //Aktif olmayan kampanyalardan bu whitelist cekmeyecek sekilde optimize et.
        val whitelistQuery ="(select REGEXP_SUBSTR(ROW_NUMBER() OVER (ORDER BY msisdn), '[^,]+', 1, 1) as increment_id ,cast(c.id as VARCHAR(100)) as id,cast(m.msisdn as VARCHAR(100)) as msisdn from msisdnlistcontent m ,campaign c, query q where c.msisdnlist_id = m.msisdnlistid and c.queryid = q.queryid and c.campaignstatus='APPROVED' and q.query_type=4) tmp"
        createDataframeFromOracle(spark,whitelistQuery,"whitelistTest","6",5) //6
      }
    },0,5,TimeUnit.HOURS)
    executor.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        println("cusblacklist running...")
        //Aktif olmayan kampanyalardan bu whitelist cekmeyecek sekilde optimize et.
        val cusblacklist ="(select REGEXP_SUBSTR(ROW_NUMBER() OVER (ORDER BY msisdn), '[^,]+', 1, 1) as increment_id ,cast(msisdn as VARCHAR(100)) as msisdn,case bl_type_name when 'CORPORATESMS' then '1'   when 'SMSBROADCAST' then '1'  end as blacklistcatid from   customerblacklist where bl_type_name='CORPORATESMS' or bl_type_name='SMSBROADCAST' ) tmp"
        createDataframeFromOracle(spark,cusblacklist,"cusblacklist","9",5) //9
      }
    },0,60,TimeUnit.HOURS)
    */
  }
}
