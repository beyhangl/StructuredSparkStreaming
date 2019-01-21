name := "SparkStKafka"

version := "0.1"

scalaVersion := "2.11.8"


libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"

// https://mvnrepository.com/artifact/RedisLabs/spark-redis
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"
// https://mvnrepository.com/artifact/oracle/ojdbc6
//libraryDependencies += "oracle" % "ojdbc6" % "11.2.0.3"
// https://mvnrepository.com/artifact/RedisLabs/spark-redis
// https://mvnrepository.com/artifact/RedisLabs/spark-redis
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.0" % "provided"


libraryDependencies += "RedisLabs" % "spark-redis" % "0.3.2" from "https://dl.bintray.com/spark-packages/maven/RedisLabs/spark-redis/0.3.2/spark-redis-0.3.2.jar"
libraryDependencies += "redis.clients" % "jedis" % "2.9.0"
libraryDependencies += "org.apache.commons" % "commons-pool2" % "2.5.0"

libraryDependencies += "redis.clients" % "jedis" % "2.4.2"


// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10


// https://mvnrepository.com/artifact/RedisLabs/spark-redis

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10


//libraryDependencies += "info.batey.kafka" % "kafka-unit" % "0.7"

//libraryDependencies += "net.sf.jopt-simple" % "jopt-simple" % "5.0.2"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.8.0"
