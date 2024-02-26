package dataGenerator

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object wordCount {
  def main(args: Array[String]): Unit = {

    val partitions = 1
    val dataper = 10000
    val name = "wordCount_10000_1"
    val seed = Random.nextLong()
    Random.setSeed(seed)

    val sparkConf = new SparkConf()
    val datasets = Array(
      ("ds1", "src/resources/dataWordCount")
    )
    sparkConf.setMaster("local[6]")
    sparkConf.setAppName("DataGen: WordCount")

    println(
      s"""
         |partitions: $partitions
         |records: $dataper
         |seed: $seed
         |""".stripMargin
    )

    datasets.foreach { case (_, f) =>
      SparkContext.getOrCreate(sparkConf).parallelize(Seq[Int](), partitions).mapPartitions { _ =>
        (1 to dataper).map { _ =>
          s"This is a sentence"
        }.iterator
      }.saveAsTextFile(f)
    }
  }

}
