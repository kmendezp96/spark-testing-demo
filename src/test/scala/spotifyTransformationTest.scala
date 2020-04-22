import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{FunSuite, GivenWhenThen}
import spark.SpotifyTransformation.{getArtistAVGPopularity, parseLine, parseLineRight}

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

  def readFileAsRDD(path: String, parseF: String => (String, String, String, String, String, Int)) = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("PopularityByArtist")
                              .setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val lines = sc.textFile(path)
    lines.map(parseF)
  }

  test("Artist popularity with parseLineRight") {

    Given("the input file is read as an rdd")
    val rdd = readFileAsRDD("src/main/resources/top10s.csv", parseLineRight)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    When("I execute the get artist popularity transformation")
    val columnsInput = Seq("id","title","artist","genre","year","popularity")
    val inputDF = rdd.toDF(columnsInput:_*)

    val results = getArtistAVGPopularity(rdd)
    results.foreach(println)

    Then("the result should meet the specified data quality characteristics")
    val columnsResult = Seq("artist","popularity")
    val outputDF = spark.createDataFrame(results).toDF(columnsResult:_*)

    assert(testDF(inputDF,outputDF))
  }

  test("Artist popularity with parseLine") {

    Given("the input file is read as an rdd")
    val rdd = readFileAsRDD("src/main/resources/top10s.csv", parseLine)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    When("I execute the get artist popularity transformation")
    val columnsInput = Seq("id","title","artist","genre","year","popularity")
    val inputDF = rdd.toDF(columnsInput:_*)

    val results = getArtistAVGPopularity(rdd)
    results.foreach(println)

    Then("the result should meet the specified data quality characteristics")
    val columnsResult = Seq("artist","popularity")
    val outputDF = spark.createDataFrame(results).toDF(columnsResult:_*)

    assert(testDF(inputDF,outputDF))
  }

}
