package analyzer

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalTime}

import org.apache.log4j.LogManager
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}

object LogAnalyzer {

  val logger = LogManager.getRootLogger

  def run(inputPrefix: String, outputPrefix: String) = {
    val spark = defaultSession
    import spark.implicits._

    val logsDF = spark.read.text(inputPrefix)
    val parsed = parseLogs(spark, logsDF)
  }

  def parseLogs(spark: SparkSession, logsDF: DataFrame) = {
    import spark.implicits._

    logsDF
      .flatMap(matchLogs)
  }

  def parseTime(time: String) = {
    val formatter = DateTimeFormatter.ofPattern("HH:mm:ss")
    Try(Timestamp.valueOf(LocalTime.parse(time, formatter).atDate(LocalDate.now())))
  }

  def parseIp(ip: String) = {
    val rex = "^(?:[0-9]{1,3}.){3}[0-9]{1,3}$".r
    Try(rex.findFirstIn(ip).get)
  }


  def matchLogs(row: Row): Option[AccessLog] = {
    val items: Array[String] = row.getAs[String]("value").split(",").map(_.trim)

    (items.lift(0), items.lift(1), items.lift(2), items.lift(3)) match {
      case (Some(ttime), Some(taction), Some(tip), Some(tuser)) => {
        (parseTime(ttime), taction, tip, tuser) match {
          case (Success(time), action, ip, user) if (action == "LOGIN") => Some(AccessLog(time, true, false, ip, user))
          case (Success(time), action, ip, user) if (action == "LOGOUT") => Some(AccessLog(time, false, true, ip, user))
          case _ => None
        }
      }
      case _ => None
    }
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

