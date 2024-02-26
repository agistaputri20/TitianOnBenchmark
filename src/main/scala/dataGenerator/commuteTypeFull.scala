package dataGenerator
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Random

object commuteTypeFull {
  def generateString(len: Int): String = {
    Random.alphanumeric.take(len).mkString
  }

  def main(args: Array[String]): Unit = {
    val partitions = 1
    val dataper = 10000
    val name = "commuteTypeFull_10000_1"
    val seed = Random.nextLong()
    Random.setSeed(seed)

    val sparkConf = new SparkConf()
    val datasets = Array(
      ("ds1", "src/resources/dataCommuteTypeFull")
    )
    sparkConf.setMaster("local[6]")
    sparkConf.setAppName("DataGen: CommuteType")

    println(
      s"""
         |partitions: $partitions
         |records: $dataper
         |seed: $seed
         |""".stripMargin
    )

    datasets.foreach { case (ds, f) =>
      SparkContext.getOrCreate(sparkConf).parallelize(Seq[Int](), partitions).mapPartitions { _ =>
        (1 to dataper).map { _ =>
          def zipcode = "9" + "0" + "0" + Random.nextInt(10).toString + Random.nextInt(10).toString

          val z1 = zipcode
          val z2 = zipcode
          val dis = Math.abs(z1.toInt - z2.toInt) * 100 + Random.nextInt(10)
          var velo = Random.nextInt(70) + 3
          if (velo <= 10) {
            if (zipcode.toInt % 100 != 1) {
              velo = velo + 10
            }
          }
          var time = dis / (velo)
          time = if (time == 0) 1 else time

          s"""sr,$z1,$z2,$dis,$time"""
        }.iterator
      }.saveAsTextFile(f)
    }
  }

}
