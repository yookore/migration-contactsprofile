package com.yookos.migration;

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.mapper._
import org.apache.commons.lang.StringEscapeUtils

case class Profile(biography: Option[String], 
                  creationdate: String, 
                  lastupdated: String,
                  relationshipstatus: Option[String],
                  username: String,
                  hometown: Option[String],
                  firstname: Option[String],
                  timezone: Option[String],
                  currentcity: Option[String],
                  userid: String,
                  lastname: Option[String],
                  birthdate: Option[String],
                  homecountry: Option[String],
                  title: Option[String],
                  currentcountry: Option[String],
                  terms: Boolean,
                  gender: Option[String],
                  imageurl: Option[String],
                  phonenumber: Option[String],
                  mobile: Option[String]) extends Serializable

object Profile {
  implicit object Mapper extends DefaultColumnMapper[Profile](
    Map("biography" -> "biography", 
      "creationdate" -> "creationdate",
      "lastupdated" -> "lastupdated",
      "relationshipstatus" -> "relationshipstatus",
      "username" -> "username",
      "hometown" -> "hometown",
      "firstname" -> "firstname",
      "timezone" -> "timezone",
      "currentcity" -> "currentcity",
      "userid" -> "userid",
      "lastname" -> "lastname",
      "birthdate" -> "birthdate",
      "homecountry" -> "homecountry",
      "title" -> "title",
      "currentcountry" -> "currentcountry",
      "terms" -> "terms",
      "gender" -> "gender",
      "imageurl" -> "imageurl",
      "phonenumber" -> "phonenumber",
      "mobile" -> "mobile"))

}
