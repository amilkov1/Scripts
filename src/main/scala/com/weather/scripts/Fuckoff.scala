package com.weather.scripts

import com.mongodb.ServerAddress
import com.mongodb.casbah._
import com.typesafe.config.Config
import com.weather.hokum.Hokum._

import scala.collection.JavaConversions._

import scalaz.Scalaz._
/**
  * Created by amilkov on 10/21/16.
  */
object Fuckoff {

  val config = getConfig("scripts", fileExtension = "conf")

  def getMongoDB(config: Config, instance: String): () => MongoDB = {
    val serverList = config.getStringList(instance + ".servers").toList.map(serverAddressToUrlAndPort)
    val dbName = config.getString(instance + ".db")
    val dbAuth =  config.hasPath(instance + ".dbAuth").option {
      val (user, pass) = authToUserAndPassword(config.getString(instance + ".dbAuth"))
      MongoCredential.createMongoCRCredential(user, instance, pass.toCharArray)
    }
    val serverAddressList = serverList.map {
      case (url, Some(port)) => new ServerAddress(url, port)
      case (url, None)       => new ServerAddress(url)
    }

    () => {
      //val mongoClientOptions = getMongoOptions(config, instance)
      val mongoClient = MongoClient(serverAddressList, dbAuth.toList, new MongoClientOptions.Builder().build)
      // TODO: Throw an exception if the dbAuth credentials are invalid.
      mongoClient.setReadPreference(ReadPreference.SecondaryPreferred)
      mongoClient(dbName)
    }
  }
  def serverAddressToUrlAndPort(address: String): (String, Option[Int]) = {
    val separatorIndex = address.indexOf(":")
    if (separatorIndex > 0) (address.substring(0, separatorIndex), Some(address.substring(separatorIndex + 1, address.length).toInt))
    else (address, None)
  }

  def authToUserAndPassword(auth: String): (String, String) = {
    val separatorIndex = auth.indexOf("@")
    (auth.substring(0, separatorIndex), auth.substring(separatorIndex+1, auth.length))
  }

}
