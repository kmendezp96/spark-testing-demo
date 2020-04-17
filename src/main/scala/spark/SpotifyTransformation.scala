package spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.{DataFrame, DataFrameNaFunctions, SparkSession}
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import org.apache.spark.rdd.RDD
import com.amazon.deequ.constraints.ConstraintStatus


object SpotifyTransformation {



  def parseLine(line: String) = {
    val fields = line.split(",")
    val id = fields(0)
    val title = fields(1)
    val artist = fields(2)
    val genre = fields(3)
    val year = fields(4)
    val popularity = fields(14).toInt
    (id, title, artist, genre, year, popularity)
  }

  def getArtistAVGPopularity(rdd: RDD[(String, String, String, String, String, Int)]) = {
    val artistPopularity = rdd.map(x => (x._3,x._6))
      .mapValues(x => (x,1))
      .reduceByKey((x,y) => (x._1 + y._1, x._2+y._2))

    val popularityAVG = artistPopularity.mapValues(x => x._1/x._2)

    popularityAVG.collect().sortWith((x,y) => x._2>y._2)
  }

  def testDF(inputDF: DataFrame, outputDF: DataFrame): Unit ={
    val verificationResult = VerificationSuite()
      .onData(outputDF)
      .addCheck(
        Check(CheckLevel.Error, "unit testing my data")
          .hasSize(_ >= 1) // we expect 5 rows
          .isComplete("artist") // should never be NULL
          .isUnique("artist") // should not contain duplicates
          .isContainedIn("artist",
            inputDF.select("artist").collect().map(_.getString(0)))
          .isComplete("popularity") // should never be NULL
          .isNonNegative("popularity")
          .hasMin("popularity",_ == 1))
      .run()

    if (verificationResult.status == CheckStatus.Success) {
      println("The data passed the test, everything is fine!")
    } else {
      println("We found errors in the data:\n")

      val resultsForAllConstraints = verificationResult.checkResults
        .flatMap { case (_, checkResult) => checkResult.constraintResults }

      resultsForAllConstraints
        .filter { _.status != ConstraintStatus.Success }
        .foreach { result => println(s"${result.constraint}: ${result.message.get}") }
    }
  }

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "PopularityByArtist")
    val lines = sc.textFile("src/main/resources/top10s.csv")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val rdd = lines.map(parseLine)


    val columnsInput = Seq("id","title","artist","genre","year","popularity")
    val inputDF = rdd.toDF(columnsInput:_*)

    val results = getArtistAVGPopularity(rdd)
    results.foreach(println)

    val columnsResult = Seq("artist","popularity")
    val outputDF = spark.createDataFrame(results).toDF(columnsResult:_*)

    testDF(inputDF,outputDF)
  }
}
