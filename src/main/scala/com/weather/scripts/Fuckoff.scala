package com.weather.scripts

import com.mongodb.ServerAddress
import com.mongodb.casbah.{MongoClient, MongoDB, ReadPreference}
import com.typesafe.config.Config
import com.weather.ssg.configuration.Hokum._
import scala.collection.JavaConversions._

/**
  * Created by amilkov on 10/21/16.
  */
object Fuckoff {

  val config = getConfig("scripts", fileExtension = "conf")

  object Mongo {
    val db = getMongoDB(config, "wxd.mongo")
  }

  def getMongoDB(config: Config, instance: String): () => MongoDB = {
    val serverList = config.getStringList(instance + ".servers").toList.map(serverAddressToUrlAndPort)
    val dbName = config.getString(instance + ".db")
    val dbAuth = if (config.hasPath(instance + ".dbAuth")) authToUserAndPassword(config.getString(instance + ".dbAuth")) else None

    val serverAddressList = serverList.map {
      case (url, Some(port)) => new ServerAddress(url, port)
      case (url, None) => new ServerAddress(url)
    }
    val mongoClient = MongoClient(serverAddressList) //MongoConnection(serverAddressList)
    mongoClient.setReadPreference(ReadPreference.SecondaryPreferred)

    () => {
      val mongoDB = mongoClient(dbName)
      dbAuth.foreach { case (user, pass) =>
        if (!mongoDB.authenticate(user, pass)) throw new Exception("UserName/Password Incorrect logging into the db!")
      }
      mongoDB
    }
  }

  def serverAddressToUrlAndPort(address: String): (String, Option[Int]) = {
    val separatorIndex = address.indexOf(":")
    if (separatorIndex > 0) (address.substring(0, separatorIndex), Some(address.substring(separatorIndex + 1, address.length).toInt))
    else (address, None)
  }

  def authToUserAndPassword(auth: String): Some[(String, String)] = {
    val separatorIndex = auth.indexOf("@")
    Some((auth.substring(0, separatorIndex), auth.substring(separatorIndex + 1, auth.length)))
  }

}
