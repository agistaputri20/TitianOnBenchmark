package dataGenerator
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Random

object customers {
  def generateString(len: Int): String = {
    Random.alphanumeric.take(len).mkString
  }

  def main(args: Array[String]): Unit = {
    val partitions = 1
    val dataper = 10000
    val name = "customers_10000_1"
    val seed = Random.nextLong()
    Random.setSeed(seed)

    val sparkConf = new SparkConf()
    val datasets = Array(
      ("ds1", "src/resources/customers/customers"),
      ("ds2", "src/resources/customers/orders")
    )
    sparkConf.setMaster("local[6]")
    sparkConf.setAppName("DataGen: Customers")

    println(
      s"""
         |partitions: $partitions
         |records: $dataper
         |seed: $seed
         |""".stripMargin
    )

    datasets.foreach { case (ds, f) =>
      ds match {
        case "ds1" =>
          SparkContext.getOrCreate(sparkConf).parallelize(Seq[Int](), partitions).mapPartitions { _ =>
            (1 to dataper).map { _ =>
              // 83804,name,location
              val cid = Random.nextInt(999)
              val name = generateString(10)
              val location = generateString(10)
              s"""$cid,$name,$location"""
            }.iterator
          }.saveAsTextFile(f)

        case "ds2" =>
          SparkContext.getOrCreate(sparkConf).parallelize(Seq[Int](), partitions).mapPartitions { _ =>
            (1 to dataper).map { _ =>
              // order82,20272,419550854,item2
              val oid = s"order${Random.nextInt(999)}"
              val cid = Random.nextInt(999)
              val time = Random.nextInt(Int.MaxValue)
              val item = s"item${Random.nextInt(Int.MaxValue)}"
              s"""$oid,$cid,$time,$item"""
            }.iterator
          }.saveAsTextFile(f)
      }
    }
  }
}
