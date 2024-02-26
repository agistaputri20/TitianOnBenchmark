package dataGenerator
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Random

object incomeAggregation {
  def randIntBetween(min: Int, max: Int): Int = {
    min + Random.nextInt((max - min) + 1)
  }

  def main(args: Array[String]): Unit = {
    val partitions = 1
    val dataper = 10000
    val name = "incomeAggregation_10000_1"
    val seed = Random.nextLong()
    Random.setSeed(seed)

    val sparkConf = new SparkConf()
    val datasets = Array(
      ("ds1", "src/resources/dataIncomeAggregation")
    )
    sparkConf.setMaster("local[6]")
    sparkConf.setAppName("DataGen: OldIncomeAggregation")

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
          // 90001,28,10990
          def zipcode: String = "9" + "0" + "0" + Random.nextInt(10).toString + Random.nextInt(10).toString

          val zip = zipcode
          val age = randIntBetween(0, 100)
          val r = Random.nextInt(10000)
          s"""$zip,$age,$r"""
        }.iterator
      }.saveAsTextFile(f)
    }
  }
}
