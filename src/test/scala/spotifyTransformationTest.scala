import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{FunSuite, GivenWhenThen}
import spark.SpotifyTransformation.{getArtistAVGPopularity, parseLineRight}

class spotifyTransformationTest extends FunSuite with GivenWhenThen {

  def testDF(inputDF: DataFrame, outputDF: DataFrame): Boolean ={
    val verificationResult = VerificationSuite()
      .onData(outputDF)
      .addCheck(
        Check(CheckLevel.Error, "unit testing my data")
          .hasSize(_ >= 1)
          .isComplete("artist")
          .isUnique("artist")
          .isContainedIn("artist",
            inputDF.select("artist").collect().map(_.getString(0)))
          .isComplete("popularity") // should never be NULL
          .isNonNegative("popularity")
          .hasMin("popularity",_ == 0))
      .run()

    if (verificationResult.status == CheckStatus.Success) {
      println("The data passed the test, everything is fine!")
      true
    } else {
      println("We found errors in the data:\n")

      val resultsForAllConstraints = verificationResult.checkResults
        .flatMap { case (_, checkResult) => checkResult.constraintResults }

      resultsForAllConstraints
        .filter { _.status != ConstraintStatus.Success }
        .foreach { result => println(s"${result.constraint}: ${result.message.get}") }
      false
    }
  }

  test("SpotifyTransformation.getArtistAVGPopularity") {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "PopularityByArtist")
    val lines = sc.textFile("src/main/resources/top10s.csv")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val rdd = lines.map(parseLineRight)


    val columnsInput = Seq("id","title","artist","genre","year","popularity")
    val inputDF = rdd.toDF(columnsInput:_*)

    val results = getArtistAVGPopularity(rdd)
    results.foreach(println)

    val columnsResult = Seq("artist","popularity")
    val outputDF = spark.createDataFrame(results).toDF(columnsResult:_*)

    assert(testDF(inputDF,outputDF))
  }

}
