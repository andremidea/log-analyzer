package analyzer

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, DatasetSuiteBaseLike}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.scalatest.{FlatSpec, Matchers}

class UserLoginSpec extends FlatSpec with DataFrameSuiteBase with DatasetSuiteBaseLike with Matchers {


  it should "return the ip with more users" in {
    import spark.implicits._

    val access: Dataset[AccessLog] =
      spark.createDataset(
        Seq(
          AccessLog(LogAnalyzer.parseTime("21:00:00").get, true, false, "10.0.0.1", "user1"),
          AccessLog(LogAnalyzer.parseTime("21:00:00").get, true, false, "10.0.0.2", "user3"),
          AccessLog(LogAnalyzer.parseTime("21:05:00").get, true, false, "10.0.0.1", "user2")))

    val topIP: Dataset[TopIP] =
      spark.createDataset(Seq(TopIP("10.0.0.1", 2), TopIP("10.0.0.2", 1)))

    val users = UserLogin.mostUsersPerIP(access)

    assertDatasetEquals(topIP, users)
  }

  it should "return the session duration for each login" in {
    import spark.implicits._

    val access: Dataset[AccessLog] =
      spark.createDataset(
        Seq(
          AccessLog(LogAnalyzer.parseTime("21:00:00").get, true, false, "10.0.0.1", "user1"),
          AccessLog(LogAnalyzer.parseTime("20:00:00").get, true, false, "10.0.0.1", "user1"),
          AccessLog(LogAnalyzer.parseTime("21:01:00").get, false, true, "10.0.0.1", "user1"),
          AccessLog(LogAnalyzer.parseTime("21:05:00").get, false, true, "10.0.0.1", "user1"),
          AccessLog(LogAnalyzer.parseTime("21:02:00").get, true, false, "10.0.0.2", "user1"),
          AccessLog(LogAnalyzer.parseTime("21:06:00").get, false, true, "10.0.0.2", "user1")))

    val expectedSessions: Dataset[Session] =
      spark.createDataset(

        Seq(Session("10.0.0.1", "user1", LogAnalyzer.parseTime("20:00:00").get, LogAnalyzer.parseTime("21:01:00").toOption, Some(3660)),
          Session("10.0.0.1", "user1", LogAnalyzer.parseTime("21:00:00").get, LogAnalyzer.parseTime("21:05:00").toOption, Some(300)),
          Session("10.0.0.2", "user1", LogAnalyzer.parseTime("21:02:00").get, LogAnalyzer.parseTime("21:06:00").toOption, Some(240)))
      )

    val sessions = UserLogin.sessions(access).sort($"start")

    assertDatasetEquals(expectedSessions, sessions)
  }


  it should "return average session duration for ip and user" in {
    import spark.implicits._

    val sessions: Dataset[Session] =
      spark.createDataset(
        Seq(Session("10.0.0.1", "user1", LogAnalyzer.parseTime("20:00:00").get, LogAnalyzer.parseTime("21:01:00").toOption, Some(3660)),
          Session("10.0.0.1", "user1", LogAnalyzer.parseTime("21:00:00").get, LogAnalyzer.parseTime("21:05:00").toOption, Some(300)),
          Session("10.0.0.2", "user2", LogAnalyzer.parseTime("21:00:00").get, LogAnalyzer.parseTime("21:06:00").toOption, Some(330)),
          Session("10.0.0.2", "user1", LogAnalyzer.parseTime("21:02:00").get, LogAnalyzer.parseTime("21:06:00").toOption, Some(240))))

    val expectedAverages: Dataset[SessionAverage] =
      spark.createDataset(
        Seq(SessionAverage("user1", "10.0.0.1", 1980),
          SessionAverage("user2", "10.0.0.2", 330),
          SessionAverage("user1", "10.0.0.2", 240)))

    val averages = UserLogin.sessionAverage(sessions).sort(-$"averageDuration")
    assertDatasetEquals(expectedAverages, averages)
  }

  it should "return the user with highest number of open sessions" in {
    import spark.implicits._

    val sessions: Dataset[Session] =
      spark.createDataset(
        Seq(Session("10.0.0.1", "user1", LogAnalyzer.parseTime("20:00:00").get, LogAnalyzer.parseTime("21:01:00").toOption, Some(3660)),
          Session("10.0.0.1", "user1", LogAnalyzer.parseTime("21:00:00").get, LogAnalyzer.parseTime("21:05:00").toOption, Some(300)),
          Session("10.0.0.1", "user3", LogAnalyzer.parseTime("21:05:00").get, LogAnalyzer.parseTime("21:08:00").toOption, Some(180)),
          Session("10.0.0.1", "user3", LogAnalyzer.parseTime("21:06:00").get, LogAnalyzer.parseTime("21:09:00").toOption, Some(180)),
          Session("10.0.0.1", "user3", LogAnalyzer.parseTime("21:07:00").get, LogAnalyzer.parseTime("21:10:00").toOption, Some(180)),
          Session("10.0.0.2", "user2", LogAnalyzer.parseTime("21:00:00").get, LogAnalyzer.parseTime("21:06:00").toOption, Some(330)),
          Session("10.0.0.2", "user1", LogAnalyzer.parseTime("21:02:00").get, LogAnalyzer.parseTime("21:06:00").toOption, Some(240))))

    val expectedOpenSessions =
      spark.createDataset(Seq(OpenSessions("user3", 3), OpenSessions("user1", 2), OpenSessions("user2",1)))

    val openSessions = UserLogin.openSessions(sessions).sort(-$"openSessions")
    assertDatasetEquals(expectedOpenSessions, openSessions)

  }
}
