package analyzer

import java.net.InetAddress
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalTime}

import analyzer.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.util.{Success, Try}

object LogParser {

  def parseLogs(logsDF: DataFrame) = {
    logsDF.flatMap(matchLogs)
  }

  def parseTime(time: String) = {
    val formatter = DateTimeFormatter.ofPattern("HH:mm:ss")
    Try(Timestamp.valueOf(LocalTime.parse(time, formatter).atDate(LocalDate.now())))
  }

  def parseIp(ip: String) = {
    Try(InetAddress.getByName(ip).getHostAddress)
  }

  def parseUser(user: String) = {
    val rex = "^[a-zA-Z0-9_]*$".r
    Try(rex.findFirstIn(user).get)
  }


  def matchLogs(row: Row): Option[AccessLog] = {
    val items: Array[String] = row.getAs[String]("value").split(",").map(_.trim)

    (items.lift(0), items.lift(1), items.lift(2), items.lift(3)) match {
      case (Some(ttime), Some(taction), Some(tip), Some(tuser)) => {
        (parseTime(ttime), taction, parseIp(tip), parseUser(tuser)) match {
          case (Success(time), action, Success(ip), Success(user)) if (action == "LOGIN") => Some(AccessLog(time, true, false, ip, user))
          case (Success(time), action, Success(ip), Success(user)) if (action == "LOGOUT") => Some(AccessLog(time, false, true, ip, user))
          case _ => None
        }
      }
      case _ => None
    }
  }
}
