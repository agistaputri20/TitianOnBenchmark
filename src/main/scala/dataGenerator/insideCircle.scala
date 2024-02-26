package dataGenerator
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Random


object insideCircle {
  def randIntBetween(min: Int, max: Int): Int = {
    min + Random.nextInt((max - min) + 1)
  }

  def main(args: Array[String]): Unit = {
    val partitions = 1
    val dataper = 10000
    val name = "insideCircle_10000_1"
    val seed = Random.nextLong()
    Random.setSeed(seed)

    val sparkConf = new SparkConf()
    val datasets = Array(
      ("ds1", "src/resources/dataInsideCircle")
    )
    sparkConf.setMaster("local[6]")
    sparkConf.setAppName("DataGen: InsideCircle")

    println(
      s"""
         |partitions: $partitions
         |records: $dataper
         |seed: $seed
         |""".stripMargin
    )

    datasets.foreach { case (_, "src/resources/dataInsideCircle") =>
      SparkContext.getOrCreate(sparkConf).parallelize(Seq[Int](), partitions).mapPartitions { _ =>
        (1 to dataper).map { _ =>
          // 90001,28,10990
          val x = randIntBetween(1, 100)
          val y = randIntBetween(1, 100)
          val r = randIntBetween(1, 2000)
          s"""$x,$y,$r"""
        }.iterator
      }.saveAsTextFile("src/resources/dataInsideCircle")
    }
  }
}
