package com.yookos.migration

import akka.actor.{ Actor, Props, ActorSystem, ActorRef }
import akka.pattern.{ ask, pipe }
import akka.event.Logging
import akka.util.Timeout

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming.{ Milliseconds, Seconds, StreamingContext, Time }
import org.apache.spark.streaming.receiver._

import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.mapper._
import com.datastax.spark.connector.cql.CassandraConnector

import org.json4s._
import org.json4s.JsonDSL._
//import org.json4s.native.JsonMethods._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}
import org.apache.commons.lang.StringEscapeUtils
import org.joda.time.DateTime

/**
 * @author ${user.name}
 */
object ContactsProfile extends App {
  
  // Configuration for a Spark application.
  // Used to set various Spark parameters as key-value pairs.
  val conf = new SparkConf(false) // skip loading external settings
  
  val mode = Config.mode
  Config.setSparkConf(mode, conf)
  val cache = Config.redisClient(mode)
  //val ssc = new StreamingContext(conf, Seconds(2))
  //val sc = ssc.sparkContext
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val system = SparkEnv.get.actorSystem
  
  if (mode == "yarn") {
    sc.addJar("hdfs:///user/hadoop-user/data/jars/postgresql-9.4-1200-jdbc41.jar")
    sc.addJar("hdfs:///user/hadoop-user/data/jars/migration-contactsprofiles-0.1-SNAPSHOT.jar")
  }
  
  createSchema(conf)
  
  implicit val formats = DefaultFormats

  val keyspace = Config.cassandraConfig(mode, Some("keyspace"))
  val totalLegacyUsers = 2124155L
  var cachedIndex = if (cache.get("latest_legacy_contacts_index") == null) 0 else cache.get("latest_legacy_contacts_index").toInt
  
  val contacts = sc.cassandraTable[Contacts](s"$keyspace", "legacyusercontacts").cache()
  // Using the mappings table, get the profiles of
  // users from 192.168.10.225 and dump to mongo
  // at 10.10.10.216
  val mappingsDF = sqlContext.load("jdbc", Map(
    "url" -> Config.dataSourceUrl(mode, Some("mappings")),
    "dbtable" -> f"(SELECT userid, cast(yookoreid as text), username FROM legacyusers offset $cachedIndex%d) as legacyusers"
    )
  )

  val legacyDF = sqlContext.load("jdbc", Map(
    "url" -> Config.dataSourceUrl(mode, Some("legacy")),
    "dbtable" -> "jiveuserprofile")
  )

  val legacyprofiles = legacyDF.select(legacyDF("fieldid"), legacyDF("value"), legacyDF("userid")).cache().collect()

  val df = mappingsDF.select(mappingsDF("userid"), mappingsDF("yookoreid"), mappingsDF("username")).cache()

  reduce(df)

  private def reduce(mdf: DataFrame) = {
    mdf.collect().foreach(row => {
      cachedIndex = cachedIndex + 1
      cache.set("latest_legacy_contacts_index", cachedIndex.toString)
      val userid = row.getLong(0)
      upsert(row, userid)
    })
  }


  private def upsert(row: Row, jiveuserid: Long) = {
    //val lp = legacyprofiles.filter(f"userid = $jiveuserid%d")
    val lp = legacyprofiles.filter(csp => csp.getLong(2) == jiveuserid)
    if (lp.length > 0) {
      lp.foreach {
        profileRow =>
          val fieldid = profileRow.getLong(0)
          val value = profileRow.getString(1)
          val username = row.getString(2)
          val userid = row.getString(1)
          val contactRDD = 
            if (contacts.count() > 0) contacts.filter(c => c.userid == userid)
              else contacts.toEmptyCassandraRDD
          val contact:Contacts = 
            if (contactRDD.count() > 0) {println("==contactRDD== " + contactRDD.first); contactRDD.first 
            }else Contacts(Some(null), Some(null), username, Some(null), Some(null), userid)

          fieldid match {
            // mobilenumber
            case 6 =>
              println(f"==fieldid:$fieldid%d and value:$value==")
              val mobilenumber = 
                if (value != null) 
                  Some(value.split("\\|")(0)) else Some(value)
                save(Seq(Contacts(
                  contact.homenumber, mobilenumber, username,
                  contact.phonenumber, contact.alternateemail, userid
                )))
   
            // phonenumber. same as workphonenumber
            case 4 => 
              println(f"==fieldid:$fieldid%d and value:$value==")
              val phonenumber = 
                if (value != null) 
                  Some(value.split("\\|")(0)) else Some(value)
                save(Seq(Contacts(
                  contact.homenumber, contact.mobilenumber, username,
                  phonenumber, contact.alternateemail, userid
              )))

            // homephonenumber
            case 5 => 
              println(f"==fieldid:$fieldid%d and value:$value==")
              val homephonenumber = 
                if (value != null) 
                  Some(value.split("\\|")(0)) else Some(value)
              save(Seq(Contacts(
                homephonenumber, contact.mobilenumber, username,
                contact.phonenumber, contact.alternateemail, userid
              )))

            // alternateemail
            case 10 => 
              save(Seq(Contacts(
                contact.homenumber, contact.mobilenumber, username,
                contact.phonenumber, contact.alternateemail, userid)))

            case _ => println("Unknown match")
          }
    
          println("===Latest ContactsProfile cachedIndex=== " + cache.get("latest_legacy_contacts_index").toInt)
      }
    }
    
  }

  def save(contact: Seq[Contacts]) = contact match {
    case Nil => println("Nil")
    case List(c @ _*) => 
      println("===new contact to save=== " + contact)
      sc.parallelize(contact)
        .saveToCassandra(s"$keyspace", "legacyusercontacts", 
          SomeColumns("homenumber", "mobilenumber", "username",
            "phonenumber", "alternateemail", "userid")
        )
  }

  mappingsDF.printSchema()
  
  def createSchema(conf: SparkConf): Boolean = {
    val keyspace = Config.cassandraConfig(mode, Some("keyspace"))
    val replicationStrategy = Config.cassandraConfig(mode, Some("replStrategy"))
    CassandraConnector(conf).withSessionDo { sess =>
      sess.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = $replicationStrategy")
      sess.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.legacyusercontacts (homenumber text, mobilenumber text, userid text, username text, phonenumber text, alternateemail text, PRIMARY KEY (userid, username)) WITH CLUSTERING ORDER BY (username DESC)")
    } wasApplied
  }
}
