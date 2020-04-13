package spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

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

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "PopularityByArtist")
    val lines = sc.textFile("src/main/resources/top10s.csv")
    val rdd = lines.map(parseLine)

    val artistPopularity = rdd.map(x => (x._3,x._6))
      .mapValues(x => (x,1))
      .reduceByKey((x,y) => (x._1 + y._1, x._2+y._2))

    val popularityAVG = artistPopularity.mapValues(x => x._1/x._2)

    val results = popularityAVG.collect().sortWith((x,y) => x._2>y._2)
    results.foreach(println)
  }
}
