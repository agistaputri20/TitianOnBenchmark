package dataGenerator
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Random

object findSalary {
  def randIntBetween(min: Int, max: Int): Int = {
    min + Random.nextInt((max - min) + 1)
  }

  def main(args: Array[String]): Unit = {
    val partitions = 1
    val dataper = 10000
    val name = "findSalary_10000_1"
    val seed = Random.nextLong()
    Random.setSeed(seed)

    val sparkConf = new SparkConf()
    val datasets = Array(
      ("ds1", "src/resources/dataFindSalary")
    )
    sparkConf.setMaster("local[6]")
    sparkConf.setAppName("DataGen: FindSalary")

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
          // 1000
          val salary = randIntBetween(100, 999999)
          val dollar = if (Random.nextBoolean() && salary > 99999) "$" else ""
          s"""$dollar$salary"""
        }.iterator
      }.saveAsTextFile(f)
    }
  }
}
