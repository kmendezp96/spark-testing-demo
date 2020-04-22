name := "spark-testing-demo"

version := "0.1"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.4.5",
  "org.apache.spark" % "spark-sql_2.11" % "2.4.5",
  "com.amazon.deequ" % "deequ" % "1.0.2",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test"
)





