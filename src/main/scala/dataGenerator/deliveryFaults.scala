package dataGenerator
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Random

object deliveryFaults {
  def generateString(len: Int): String = {
    Random.alphanumeric.take(len).mkString
  }

  def main(args: Array[String]): Unit = {
    val partitions = 1
    val dataper = 10000
    val name = "deliveryFaults_10000_1"
    val seed = Random.nextLong()
    Random.setSeed(seed)

    val sparkConf = new SparkConf()
    val datasets = Array(
      ("ds1", "src/resources/dataDeliveryFaults")
    )
    sparkConf.setMaster("local[6]")
    sparkConf.setAppName("DataGen: DeliveryFaults")

    println(
      s"""
         |partitions: $partitions
         |records: $dataper
         |seed: $seed
         |""".stripMargin
    )

    datasets.foreach { case (_, "src/resources/dataDeliveryFaults") =>
      SparkContext.getOrCreate(sparkConf).parallelize(Seq[Int](), partitions).mapPartitions { _ =>
        (1 to dataper).map { _ =>
          // dlvry58490,cust44067,vend57550,5
          val did = s"dlvry${Random.nextInt(99999)}"
          val cid = s"cust${Random.nextInt(99999)}"
          val vend = s"vend${Random.nextInt(99999)}"
          val rating = Random.nextInt(6) + 1
          s"""$did,$cid,$vend,$rating"""
        }.iterator
      }.saveAsTextFile("src/resources/dataDeliveryFaults")

    }
  }
}
