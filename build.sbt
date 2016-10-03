name := "log-analyzer"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.0.0" % "provided",
  "org.apache.spark"  %% "spark-hive" % "2.0.0" % "provided",
  "org.slf4j" % "slf4j-api" % "1.7.21",
  "org.scalatest" %% "scalatest" % "2.2.6",
  "com.holdenkarau" %% "spark-testing-base" % "2.0.0_0.4.4" excludeAll ExclusionRule(organization="org.apache.hadoop"))


parallelExecution in Test := false
fork in Test := true
