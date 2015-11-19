package com.yookos.migration

import org.apache.spark._
import scala.util.parsing.json._
import collection.mutable.WrappedArray

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

object Config {
  // if env var HOSTNAME is set to sandbox, then it's staging
  // if VCAP_APPLICATION env var is set, then it's CF
  // else local
  val env = Map("sandbox" -> System.getenv("HOSTNAME"), "cf" -> System.getenv("VCAP_APPLICATION"), "yarn" -> System.getenv("YARN_HOME"))

  println("====VCAP_APPLICATION====\n " + env.get("cf"))

  val mode = initEnv(env)

  def initEnv(mode: Map[String, String]): String = {
    implicit val formats = DefaultFormats
    val cf = mode.get("cf")
    if (mode.get("sandbox") == Some("sandbox")) {
      Some("sandbox").get
    }
    else if(cf != Some(null)) {
      //val vcap = JSON.parseFull(Some(cf).get.asInstanceOf[String])
      //val m = parse(cf.get.toString).extract[Map[String, String]]
      val m = JSON.parseFull(cf.get).get.asInstanceOf[Map[String, String]]
      println("===mappedJson===: " + m)
      val spaceName = m.get("space_name").get
      println("===spaceName===: " + spaceName)
      spaceName match {
        case "dev" => spaceName
        case "beta" => spaceName
      }
      //spaceName.get
    }
    else if(env.get("yarn") != Some(null)) {
      Some("yarn").get
    }
    else {
      //if (System.getenv("BETA_ON_LOCAL") == "beta") "beta" else Some("local").get
      Some("local").get
    }
  }

  def setSparkConf(env: String, conf: SparkConf) = env match {
    // set based on environment
    case "local" => 
      val driverPort = 7077
      val driverHost = "localhost"
      //val masterUrl = "spark://" + driverHost + ":" + driverPort
      conf.setAppName("Yookore User Account Analytics")
      conf.setMaster("local[*]")
      conf.set("spark.driver.port", driverPort.toString)
      conf.set("spark.driver.host", driverHost)
      conf.set("spark.logConf", "true")
      conf.set("spark.akka.logLifecycleEvents", "true")
      conf.set("spark.driver.allowMultipleContexts", "true")
      //conf.set("spark.cassandra.connection.host", "localhost")
      //conf.set("spark.cassandra.connection.host", "192.168.10.200")
      //conf.set("spark.cassandra.auth.username", "cassandra")
      //conf.set("spark.cassandra.auth.password", "cassandra")
      conf.set("spark.cassandra.connection.host", "192.168.121.174")
      conf.set("spark.cassandra.auth.username", "cassandra")
      conf.set("spark.cassandra.auth.password", "Gonzo@7072")

    case "sandbox" => 
      val driverPort = 7077
      val driverHost = "10.10.10.100"
      // By default Spark would use total cores and total RAM
      // on the host machine.
      conf.setAppName("Legacy User Profiles Migration")
      conf.setMaster("local[*]")
      conf.set("spark.logConf", "true")
      conf.set("spark.akka.logLifecycleEvents", "true")
      conf.set("spark.driver.allowMultipleContexts", "true")
      conf.set("spark.cassandra.connection.host", "192.168.10.200")
      conf.set("spark.cassandra.auth.username", "cassandra")
      conf.set("spark.cassandra.auth.password", "cassandra")
      //.set("spark.driver.port", "58522")

    case "dev" =>
      val driverPort = 7077
      val driverHost = "10.10.10.100"
      conf.setAppName("Legacy User Profiles Migration")
      //conf.setMaster("yarn-client")
      conf.setMaster("local[*]")
      conf.set("spark.logConf", "true")
      conf.set("spark.akka.logLifecycleEvents", "true")
      conf.set("spark.driver.allowMultipleContexts", "true")
      conf.set("spark.cassandra.connection.host", "192.168.10.200")
      conf.set("spark.cassandra.auth.username", "cassandra")
      conf.set("spark.cassandra.auth.password", "cassandra")
    
    case "beta" =>
      println("===Running in beta mode===")
      val driverPort = 7077
      val driverHost = "192.168.121.160"
      conf.setAppName("Legacy User Profiles Migration")
      //conf.setMaster("yarn-client")
      conf.setMaster("local[*]")
      conf.set("spark.logConf", "true")
      conf.set("spark.akka.logLifecycleEvents", "true")
      conf.set("spark.driver.allowMultipleContexts", "true")
      // Uses all cores by default
      //conf.set("spark.executor.cores", "4")
      //conf.set("spark.driver.maxResultSize", "0")
      //conf.set("spark.driver.memory", "2g")
      //conf.set("spark.executor.memory", "6g")
      conf.set("spark.cassandra.connection.host", "192.168.121.174")
      conf.set("spark.cassandra.auth.username", "cassandra")
      conf.set("spark.cassandra.auth.password", "Gonzo@7072")

    case "yarn" =>
      println("===Running directly on Spark master mode===")
      val driverPort = 7077
      val driverHost = "192.168.121.160"
      conf.setAppName("Legacy Files Migration")
      conf.setMaster("yarn-client")
      conf.set("spark.logConf", "true")
      conf.set("spark.akka.logLifecycleEvents", "true")
      //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //conf.set("spark.kryoserializer.buffer.max", "1024m")
      //conf.set("spark.driver.allowMultipleContexts", "true")
      conf.set("spark.yarn.am.cores", "2")
      //conf.set("spark.driver.maxResultSize", "0")
      conf.set("spark.executor.memory", "5g") //12g
      conf.set("spark.executor.cores", "8") // 8
      conf.set("spark.executor.instances", "6") //13
      conf.set("spark.yarn.access.namenodes", "hdfs://yks-hadoop-m01:8032")
      conf.set("spark.cassandra.connection.host", "192.168.121.174")
      conf.set("spark.cassandra.auth.username", "cassandra")
      conf.set("spark.cassandra.auth.password", "Gonzo@7072")

    case "production" =>
      conf.setAppName("Yookore User Account Analytics")
      conf.setMaster("yarn-client")
      conf.set("spark.logConf", "true")
      conf.set("spark.akka.logLifecycleEvents", "true")
      conf.set("spark.driver.allowMultipleContexts", "true")
      conf.set("spark.cassandra.connection.host", "192.168.10.200")
      conf.set("spark.cassandra.auth.username", "cassandra")
      conf.set("spark.cassandra.auth.password", "cassandra")
      
  }

  /*import com.redis.RedisClient
  import com.redis.cluster._
  import com.redis.serialization.Format*/

  import redis.clients.jedis._

  def redisClient(env: String) = env match {
    case "local" => 
      val jedis = new Jedis("localhost");
      jedis

    case "dev" =>
      var jedisClusterNodes = new java.util.HashSet[HostAndPort]
      //val jedisClusterNodes = Set[HostAndPort]()
      jedisClusterNodes.add(new HostAndPort("192.168.10.4", 6379))
      jedisClusterNodes.add(new HostAndPort("192.168.10.5", 6379))
      jedisClusterNodes.add(new HostAndPort("192.168.10.98", 6379))
      val jc = new JedisCluster(jedisClusterNodes);
      jc
    
    case "beta" =>
      val jedisClusterNodes = new java.util.HashSet[HostAndPort]
      //val jedisClusterNodes = Set[HostAndPort]()
      jedisClusterNodes.add(new HostAndPort("192.168.121.165", 6379))
      jedisClusterNodes.add(new HostAndPort("192.168.121.166", 6379))
      jedisClusterNodes.add(new HostAndPort("192.168.121.167", 6379))
      val jc = new JedisCluster(jedisClusterNodes);
      jc
    
    case "yarn" =>
      val jedisClusterNodes = new java.util.HashSet[HostAndPort]
      //val jedisClusterNodes = Set[HostAndPort]()
      jedisClusterNodes.add(new HostAndPort("192.168.121.165", 6379))
      jedisClusterNodes.add(new HostAndPort("192.168.121.166", 6379))
      jedisClusterNodes.add(new HostAndPort("192.168.121.167", 6379))
      val jc = new JedisCluster(jedisClusterNodes);
      jc
  }

  def dataSourceUrl(env: String, name: Option[String]): String = env match {
    case "local" => 
      val dbSourceName = name.getOrElse("")
      //val mappings = s"jdbc:postgresql://localhost:5432/uaa?user=root&password=P@ssw0rd15"
      //val legacy = s"jdbc:postgresql://localhost:5432/uaa?user=root&password=P@ssw0rd15"
      val mappings = s"jdbc:postgresql://10.10.10.227:5432/uaa?user=postgres&password=postgres"
      val legacy = s"jdbc:postgresql://192.168.10.225:5432/yookos?user=postgres&password=postgres"
      if (dbSourceName == "mappings") mappings else legacy

    case "dev" => 
      val dbSourceName = name.getOrElse("")
      val mappings = s"jdbc:postgresql://10.10.10.227:5432/uaa?user=postgres&password=postgres"
      val legacy = s"jdbc:postgresql://192.168.10.225:5432/yookos?user=postgres&password=postgres"
      if (dbSourceName == "mappings") mappings else legacy

    case "sandbox" =>
      val dbSourceName = name.getOrElse("")
      val mappings = s"jdbc:postgresql://10.10.10.227:5432/uaa?user=postgres&password=postgres"
      val legacy = s"jdbc:postgresql://192.168.10.225:5432/yookos?user=postgres&password=postgres"
      if (dbSourceName == "mappings") mappings else legacy
    
    case "beta" =>
      val dbSourceName = name.getOrElse("")
      val mappings = s"jdbc:postgresql://192.168.121.178:5432/uaa?user=postgres&password=postgres"
      val legacy = s"jdbc:postgresql://192.168.121.164:5432/Yookos?user=postgres&password=postgres"
      if (dbSourceName == "mappings") mappings else legacy
    
    case "yarn" =>
      val dbSourceName = name.getOrElse("")
      val mappings = s"jdbc:postgresql://192.168.121.178:5432/uaa?user=postgres&password=postgres"
      val legacy = s"jdbc:postgresql://192.168.121.164:5432/Yookos?user=postgres&password=postgres"
      if (dbSourceName == "mappings") mappings else legacy
  }

  import reactivemongo.api._
  import scala.concurrent.ExecutionContext.Implicits.global

  def mongo(): DefaultDB = {
  
    val driver = new MongoDriver
    val connection = driver.connection(List("10.10.10.216"))

    // Gets a reference to the database "plugin"
    val db = connection("jiveuserprofile")
    db
  }
  
  def cassandraConfig(env: String, name: Option[String]): String = env match {

    case "local" =>
      val param = name.getOrElse("")
      param match {
        //case "keyspace" => s"yookore"
        //case "replStrategy" => "{'class': 'SimpleStrategy', 'replication_factor': 3}"
        case "keyspace" => s"yookos_migration"
        case "replStrategy" => "{'class': 'NetworkTopologyStrategy', 'DC1': 3}"
      }

    case "dev" =>
      val param = name.getOrElse("")
      param match {
        case "keyspace" => s"yookore"
        case "replStrategy" => "{'class': 'SimpleStrategy', 'replication_factor': 3}"
      }

    case "beta" =>
      val param = name.getOrElse("")
      param match {
        case "keyspace" => s"yookos_migration"
        case "replStrategy" => "{'class': 'NetworkTopologyStrategy', 'DC1': 3}"
      }
    
    case "yarn" =>
      val param = name.getOrElse("")
      param match {
        case "keyspace" => s"yookos_migration"
        case "replStrategy" => "{'class': 'NetworkTopologyStrategy', 'DC1': 3}"
      }
  }
}
