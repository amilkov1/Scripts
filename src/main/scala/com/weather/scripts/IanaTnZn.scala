package com.weather.scripts

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.Imports._
import com.weather.ssg.configuration.Hokum._
import org.json4s.jackson.JsonMethods._
import org.json4s._
import com.weather.scripts.HttpClientUtil._

/**
  * Created by amilkov on 10/21/16.
  */
object IanaTnZn {

  implicit val formats = DefaultFormats

  def main(args: Array[String]) : Unit = {
    addIanas
  }

  def addIanas = {
    val wxd = Fuckoff.getMongoDB(Fuckoff.config, "wxd.mongo")()
    wxd.addOption(16)
    //("ZFRecord", "ZFData", "ZFData")
    List(("ZFRecord", "ZFData", "ZFData")).foreach{ case(rec, country, target) =>
      wxd(rec).find(MongoDBObject("ZFData.ianaTmZn" -> MongoDBObject("$exists"-> false))).foreach { l =>
        val lat = Encoder.getString(l, Array(country, "lat")).get
        val long = Encoder.getString(l, Array(country, "long")).get
        callLocService(lat, long) match {
          case Some(x) =>
            Encoder.getObject(l,  Array(target)).asInstanceOf[DBObject].update("ianaTmZn", x)
            wxd(rec).save(l, WriteConcern.Normal)
          case None => {
            println("bruh loc service doesnt have an iana for this lat long " + lat + " " + long)
            println(rec + " " + Encoder.getString(l, Array("_id")).get)
          }
        }
      }
    }
  }
  def callLocService(lat: String, long: String) = {

      import LocationService._

      val uri = builder
        .setParameter("geocode", lat + "," + long)
        .build

      val resp = httpGet(httpClient, uri)

      resp.getStatusLine.getStatusCode match {
        case 200 => parseOpt(resp.getEntity.getContent).flatMap(x => findOne(x, "key").extractOpt[String])
        case x => None
      }

  }

  def findOne(orgJson: JValue, nameToFind: String) : JValue = {
    orgJson.findField { case (x, _) => x == nameToFind} match {
      case Some((_, y)) => y
      case None => JNothing
    }
  }
}


private object LocationService {


  val config = getConfig("scripts", fileExtension = "conf")

  val httpClient = httpClientBuilder.build

  val prefix = "location.dst.rest.command.service"

  val host = config.getString(prefix + ".host")
  val port = config.getInt(prefix + ".port")
  val path = config.getString(prefix + ".path")
  val builder = uriBuilder("http", host, port, path)
    .setParameter("format", config.getString(prefix + ".format"))
      .setParameter("product", config.getString(prefix + ".product"))
    .setParameter("apiKey", config.getString(prefix + ".apiKey"))
}
