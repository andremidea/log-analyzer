package analyzer

import java.sql.Timestamp

import org.apache.spark.sql.SQLImplicits

case class AccessLog(time: Timestamp, login: Boolean, logout: Boolean, ip: String, user: String)

case class TopIP(ip: String, users: Long)

case class Session(ip: String, user: String, start: Timestamp, end: Option[Timestamp], duration: Option[Long])

case class SessionAverage(user: String, ip: String, averageDuration: Double)

case class OpenSessions(user: String, openSessions: Long)

object implicits extends SQLImplicits {
  // so i don't need to import spark.implicits._ everywhere
  protected override def _sqlContext = null
}