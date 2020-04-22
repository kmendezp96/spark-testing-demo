package spark

import org.apache.spark.rdd.RDD

object SpotifyTransformation extends App{

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

  def parseLineRight(line: String) = {
    val regex = """(",")|(",)|,(?=\d)|,(?=-)""";
    val fields = line.split(regex)
    val id = fields(0).replaceFirst("\"","")
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
}
