package com.yookos.migration;

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.mapper._
import org.apache.commons.lang.StringEscapeUtils

case class Contacts(homenumber: Option[String], 
                  mobilenumber: Option[String],
                  username: String,
                  phonenumber: Option[String],
                  alternateemail: Option[String],
                  userid: String
                  ) extends Serializable

object Contacts {
  implicit object Mapper extends DefaultColumnMapper[Contacts](
    Map("homenumber" -> "homenumber", 
      "mobilenumber" -> "mobilenumber",
      "username" -> "username",
      "phonenumber" -> "phonenumber",
      "alternateemail" -> "alternateemail",
      "userid" -> "userid"))

}
