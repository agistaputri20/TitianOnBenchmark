package dataGenerator
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Random

object numberSeries {

  def randIntBetween(min: Int, max: Int): Int = {
    min + Random.nextInt((max - min) + 1)
  }

  def generateString(len: Int): String = {
    Random.alphanumeric.take(len).mkString
  }

  def main(args: Array[String]): Unit = {
    val partitions = 1
    val dataper = 10000
    val name = "numberSeries_10000_1"
    val seed = Random.nextLong()
    Random.setSeed(seed)

    val sparkConf = new SparkConf()
    val datasets = Array(
      ("ds1", "src/resources/dataNumberSeries")
    )
    sparkConf.setMaster("local[6]")
    sparkConf.setAppName("DataGen: NumberSeries")

    println(
      s"""
         |partitions: $partitions
         |records: $dataper
         |seed: $seed
         |""".stripMargin
    )

    datasets.foreach { case (_, "src/resources/dataNumberSeries") =>
      SparkContext.getOrCreate(sparkConf).parallelize(Seq[Int](), partitions).mapPartitions { _ =>
        (1 to dataper).map { _ =>
          // 32,234
          val n1 = randIntBetween(1, 500)
          val n2 = randIntBetween(1, 500)
          s"""$n1,$n2"""
        }.iterator
      }.saveAsTextFile("src/resources/dataNumberSeries")
    }
  }


}
