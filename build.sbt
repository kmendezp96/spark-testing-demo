name := "spark-testing-demo"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.12" % "3.0.0-preview2",
  "org.apache.spark" % "spark-sql_2.12" % "3.0.0-preview2",
  "com.amazon.deequ" % "deequ" % "1.0.2"
)



