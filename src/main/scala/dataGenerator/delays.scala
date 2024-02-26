package dataGenerator
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Random

object delays {
  def main(args: Array[String]): Unit = {
    val partitions = 1
    val dataper = 10000
    val name = "delays_10000_1"
    val seed = Random.nextLong()
    Random.setSeed(seed)

    val sparkConf = new SparkConf()
    val datasets = Array(
      ("ds1", "src/resources/dataStation1"),
      ("ds2", "src/resources/dataStation2")
    )
    sparkConf.setMaster("local[6]")
    sparkConf.setAppName("DataGen: Delays")

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
          // trip58490,95880023,68370870,route3
          val tripId = s"trip${Random.nextInt(99999)}"
          val a = Random.nextInt(Int.MaxValue)
          val d = Random.nextInt(Int.MaxValue)
          val r = s"route${Random.nextInt(100)}"
          s"""$tripId,$a,$d,$r"""
        }.iterator
      }.saveAsTextFile(f)
    }
  }
}
