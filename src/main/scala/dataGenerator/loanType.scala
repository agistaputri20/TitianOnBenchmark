package dataGenerator
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Random

object loanType {
  def generateString(len: Int): String = {
    Random.alphanumeric.take(len).mkString
  }

  def randIntBetween(min: Int, max: Int): Int = {
    min + Random.nextInt((max - min) + 1)
  }

  def randFloatBetween(min: Int, max: Int): Float = {
    (randIntBetween(min * math.pow(10.0, 4).toInt, max * math.pow(10.0, 4).toInt) / math.pow(10.0, 4)).toFloat
  }

  def main(args: Array[String]): Unit = {
    val partitions = 1
    val dataper = 10000
    val name = "loanType_10000_1"
    val seed = Random.nextLong()
    Random.setSeed(seed)

    val sparkConf = new SparkConf()
    val datasets = Array(
      ("ds1", "src/resources/dataLoanType")
    )
    sparkConf.setMaster("local[6]")
    sparkConf.setAppName("DataGen: LoanType")

    println(
      s"""
         |partitions: $partitions
         |records: $dataper
         |seed: $seed
         |""".stripMargin
    )

    datasets.foreach { case (_, "src/resources/dataLoanType") =>
      SparkContext.getOrCreate(sparkConf).parallelize(Seq[Int](), partitions).mapPartitions { _ =>
        (1 to dataper).map { _ =>
          // 3400.0,40,0.0211,Amy
          val id = randIntBetween(1000, 9999)
          val years = randIntBetween(10, 50)
          val rate = randFloatBetween(0, 1)
          val name = generateString(10)
          s"""$id,$years,$rate,$name"""
        }.iterator
      }.saveAsTextFile("src/resources/dataLoanType")
    }
  }
}
