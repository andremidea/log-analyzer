package analyzer

import scala.util.{Failure, Success, Try}

import org.apache.log4j.LogManager
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object LogAnalyzer {

  val logger = LogManager.getRootLogger

  def run(inputPrefix: String, outputPrefix: String) = {
    val spark = defaultSession
    import spark.implicits._

    val logsDF = spark.read.text(inputPrefix)
    val access = LogParser.parseLogs(logsDF)
    access.write.csv(s"${outputPrefix}/logs.csv")

    val mostUsersPerIp = LoginAnalyses.mostUsersPerIP(access)
    val sessions = LoginAnalyses.sessions(access)
    val higestOpenSessions = LoginAnalyses.openSessions(sessions)
    val averageSessions = LoginAnalyses.sessionAverage(sessions)

    mostUsersPerIp.sort(-$"users").repartition(1).write.csv(s"${outputPrefix}/most-users-per-ip.csv")
    higestOpenSessions.sort(-$"openSessions").repartition(1).write.csv(s"${outputPrefix}/open-sessions.csv")
    averageSessions.sort(-$"averageDuration").repartition(1).write.csv(s"${outputPrefix}/average-session-duration.csv")
  }

  def defaultSession: SparkSession = {
    SparkSession.builder()
      .appName("Log Analyzer")
      .config("spark.rdd.compressed", "true")
      .config("spark.sql.parquet.compression.codec", "gzip")
      .config("mapreduce.fileoutputcommitter.algorithm.version", 2)
      .getOrCreate()
  }

  def parseAndRun(argv: Array[String]): Try[Unit] = argv match {
    case Array(inputFile, outputPrefix) =>
      Try(run(inputFile, outputPrefix))
    case _ =>
      Failure(
        throw new IllegalArgumentException(
          "java -jar job.jar <inputPrefix> <outputPrefix>"))
  }

  def main(argv: Array[String]): Unit = parseAndRun(argv) match {
    case Success(_) => System.exit(0)
    case Failure(exception) =>
      logger.error("An error occurred", exception)
      System.exit(-1)
  }
}

