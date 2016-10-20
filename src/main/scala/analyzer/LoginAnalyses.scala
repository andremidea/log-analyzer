package analyzer

import java.sql.Timestamp
import java.time.Instant

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import analyzer.implicits._

object LoginAnalyses {

  val timestampDiff = udf((t1: Timestamp, t2: Timestamp) => {
    (t2.getTime - t1.getTime) / 1000
  })

  def mostUsersPerIP(access: Dataset[AccessLog]) = {
    access.toDF()
      .where($"login" === true)
      .groupBy($"ip")
      .agg(countDistinct($"user").as("users"))
      .sort(-$"users")
      .as[TopIP]
  }


  def sessions(access: Dataset[AccessLog]) = {
    val win = Window.partitionBy($"user", $"ip")
      .orderBy($"time")

    val logouts = access.toDF()
      .where($"logout" === true)
      .withColumn("row", row_number().over(win))
      .as("logout")

    val login = access.toDF()
      .where($"login" === true)
      .withColumn("row", row_number().over(win))
      .as("login")

    login
      .join(logouts, $"login.ip" === $"logout.ip"
        and $"login.user" === $"logout.user"
        and $"login.row" === $"logout.row"
        , "left")
      .withColumn("end", $"logout.time")
      .withColumn("duration", timestampDiff($"login.time", $"end"))
      .select($"login.ip", $"login.user", $"login.time".as("start"), $"end", $"duration")
      .as[Session]
  }


  def sessionAverage(sessions: Dataset[Session]) = {
    sessions.toDF()
      .groupBy($"user", $"ip")
      .agg(avg($"duration").as("averageDuration"))
      .as[SessionAverage]
  }

  def openSessions(sessions: Dataset[Session]) = {
    sessions
      .groupByKey(_.user)
      .mapGroups(currentOpenSessions)
  }

  def currentOpenSessions(user: String, sessions: Iterator[Session]): OpenSessions = {
    val defaultTime = Timestamp.from(Instant.now())
    val allSessions = sessions.toList //naive solution, all sessions for a given user must fit in memory

    allSessions
      .foldLeft(OpenSessions(user, 0))((acc, cs) => {
        val currentSessions = allSessions
          .filter(s => {
            (s.start.compareTo(cs.start) <= 0) &&
              s.end.getOrElse(defaultTime)
                .after(cs.start)
          }).size

        if (acc.openSessions >= currentSessions)
          acc
        else
          acc.copy(openSessions = currentSessions)
      })
  }

}
