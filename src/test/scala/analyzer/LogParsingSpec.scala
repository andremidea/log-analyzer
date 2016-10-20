package analyzer

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, DatasetSuiteBaseLike}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.scalatest.{FlatSpec, Matchers}

class LogParsingSpec extends FlatSpec with DataFrameSuiteBase with DatasetSuiteBaseLike with Matchers {

  "when parsing" should "ignored non matching logs" in {

    import spark.implicits._

    val expectedLogs: Dataset[AccessLog] =
      spark.createDataset(
        Seq(
          AccessLog(LogParser.parseTime("21:00:00").get, true, false, "10.0.0.1", "user1"),
          AccessLog(LogParser.parseTime("21:05:00").get, false, true, "10.0.0.1", "user1")))


    val source = this.getClass.getResource("/simple_log.csv")
    val logsDF = spark.read.text(source.getFile)

    val logs: Dataset[AccessLog] = LogParser.parseLogs(logsDF)

    assertDatasetEquals(expectedLogs, logs)
  }

}
