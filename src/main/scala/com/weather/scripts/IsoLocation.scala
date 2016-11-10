/**
  * Created by amilkov on 11/8/16.

  */
package com.weather.scripts

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.Imports._
import com.weather.hokum.Hokum._
import com.weather.scripts.HttpClientUtil._
import org.json4s._
import org.json4s.jackson.JsonMethods._

object IsoLocation {

  implicit val formats = DefaultFormats

  val col = "services_data"

  val geoReg = "-*[0-9]{2,3}.[0-9]{1,2},-*[0-9]{2,3}.[0-9]{1,2}".r

  def main(args: Array[String]) = {
    convDeZoneId
  }

  val popMap = {
    val ccMap = scala.collection.mutable.Map[String, String]()
    for (l <- scala.io.Source.fromFile("/Users/amilkov/Google Drive/Scripts/src/main/resources/etc/countries.txt").getLines) {
      val ls = l.split(",")
      if (ls.length >= 2 && ls(0) != ls(1)) ccMap += ls(0) -> ls(1)
    }
    ccMap
  }

  def convLocs = {
    val ups = Fuckoff.getMongoDB(Fuckoff.config, "ups.mongo")()
    ups.addOption(16)
    ups(col).find(MongoDBObject("cc"-> MongoDBObject("$exists"-> true))).foreach { l =>
      writeIsoCc(l, ups)
    }
  }

  def writeIsoCc(l: DBObject, ups: MongoDB) = for {
    cc <- l.getAs[String]("cc")
    iso <- popMap.get(cc)
    _ = l.update("ccIso", iso)
    _ = l.removeField("cc")
  } yield ups(col).save(l, WriteConcern.Normal)


  def convDeZoneId = {
    val ups = Fuckoff.getMongoDB(Fuckoff.config, "ups.mongo")()
    ups.addOption(16)
    ups(col).find(MongoDBObject("cc"-> "GM")).foreach { l =>
      for {
        loc <- l.getAs[String]("out", "loc", "loc")
        json <- callService(loc)
        zoneId <- extractZone(json)
        _ = l.update("zoneId", zoneId)
      } yield writeIsoCc(l, ups)
    }
  }

  def extractZone(json: JValue) = for {
    keys <- (json \ "keys").extractOpt[Array[String]]
    Array(countyId, zoneId) = keys
  } yield zoneId

  def callGeohitService(lat: String, long: String) = {

      import GeoHit._

      val uri = builder
        .setParameter("geocode", lat + "," + long)
        .build

      val resp = httpGet(httpClient, uri)

      resp.getStatusLine.getStatusCode match {
        case 200 => parseOpt(resp.getEntity.getContent)
        case x => None
      }

  }


  def callService(loc: String) = {
    geoReg.findFirstIn(loc) match {
        case Some(x) =>
          val arr = x.split(",")
          callGeohitService(arr(0), arr(1))

        case None =>
          val (lat, lon) = extractLatLon(callWxdLoc(loc).get).get
          callGeohitService(lat.toString, lon.toString)
    }
  }



  /*def callResolveService(loc: String) = {



      import ResolveService._

      val uri = builder
        .setParameter("id", loc)
        .build

      val resp = httpGet(httpClient, uri)

      resp.getStatusLine.getStatusCode match {
        case 200 => parseOpt(resp.getEntity.getContent)
        case x => None
      }

  }*/

  def callWxdLoc(loc: String) = {

    import WxdService._

    val uri = builder.setPath("/wxd/loc/" + loc).build

    val resp = httpGet(httpClient, uri)


    resp.getStatusLine.getStatusCode match {
      case 200 => parseOpt(resp.getEntity.getContent)
      case x => None
    }


  }

  def extractLatLon(json : JValue) = for {
    lat <- (json \ "lat").extractOpt[Double]
    lon <- (json \ "long").extractOpt[Double]
  } yield (lat, lon)

  /*
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
   */

}


/*private object ResolveService {


  val config = getConfig("scripts", fileExtension = "conf")

  val httpClient = httpClientBuilder.build

  val prefix = "location.dst.rest.command.service"

  val host = config.getString(prefix + ".host")
  val port = config.getInt(prefix + ".port")
  val path = config.getString(prefix + ".path")
  val builder = uriBuilder("http", host, port, path)
    .setParameter("format", config.getString(prefix + ".format"))
      .setParameter("product", config.getString("country"))
    .setParameter("apiKey", config.getString(prefix + ".apiKey"))
      .setParameter("type", "alerts")
}*/

private object WxdService {
  val config = getConfig("scripts", fileExtension = "conf")

  val httpClient = httpClientBuilder.build

  val prefix = "wxd.loc.rest.command.service"

  val host = config.getString(prefix + ".host")
  val port = config.getInt(prefix + ".port")
  val builder = uriBuilder("http", host, port)
}

private object GeoHit {


  val config = getConfig("scripts", fileExtension = "conf")

  val httpClient = httpClientBuilder.build

  val prefix = "location.dst.rest.command.service"

  val host = config.getString(prefix + ".host")
  val port = config.getInt(prefix + ".port")
  val path = config.getString(prefix + ".geohitPath")
  val builder = uriBuilder("http", host, port, path)
    .setParameter("format", config.getString(prefix + ".format"))
      .setParameter("product", "alerts")
    .setParameter("apiKey", config.getString(prefix + ".apiKey"))
}


