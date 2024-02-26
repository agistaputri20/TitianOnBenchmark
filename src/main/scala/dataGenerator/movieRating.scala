package dataGenerator
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object movieRating {
  def randIntBetween(min: Int, max: Int): Int = {
    min + Random.nextInt((max - min) + 1)
  }

  def generateString(len: Int): String = {
    Random.alphanumeric.take(len).mkString
  }

  def main(args: Array[String]): Unit = {
    val partitions = 1
    val dataper = 10000
    val name = "movieRating_10000_1"
    val seed = Random.nextLong()
    Random.setSeed(seed)

    val sparkConf = new SparkConf()
    val datasets = Array(
      ("ds1", "src/resources/dataMovieRating")
    )
    sparkConf.setMaster("local[6]")
    sparkConf.setAppName("DataGen: MovieRating")

    println(
      s"""
         |partitions: $partitions
         |records: $dataper
         |seed: $seed
         |""".stripMargin
    )

    datasets.foreach { case (_, "src/resources/dataMovieRating") =>
      SparkContext.getOrCreate(sparkConf).parallelize(Seq[Int](), partitions).mapPartitions { _ =>
        (1 to dataper).map { _ =>
          // ASTAR,2
          val movie = generateString(10)
          val rating = randIntBetween(0, 100)
          s"""$movie,$rating"""
        }.iterator
      }.saveAsTextFile("src/resources/dataMovieRating")
    }
  }

}
