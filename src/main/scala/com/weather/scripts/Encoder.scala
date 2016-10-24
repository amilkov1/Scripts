//Copyright © 2013 The Weather Channel. All rights are reserved.
package com.weather.scripts

import com.mongodb.casbah.Imports._
import com.mongodb.util.JSON
import com.typesafe.config.Config
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.io.Source
import scala.xml.pull.{EvElemStart, EvText, _}

object Encoder {

  @tailrec
  def getString(dbo:DBObject, pathList: Array[String]): Option[String] = {
    if (pathList.length.equals(1)) Option(dbo.get(pathList.head)).map(_.toString)
    else {
      Option(dbo.get(pathList.head)) match {
        case Some(d : DBObject) => getString(d, pathList.tail)
        case None => None
      }
    }
  }

  @tailrec
  def getObject(dbo: DBObject, pathList: Array[String]): Any = {
    if (pathList.length.equals(1)) dbo.get(pathList.head)
    else {
      Option(dbo.get(pathList.head)) match {
        case Some(d : DBObject) => getObject(d, pathList.tail)
        case None => throw new Exception("Key does not exist")
      }

    }
  }

}

