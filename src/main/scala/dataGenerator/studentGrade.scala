package dataGenerator
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Random


object studentGrade {
  def randIntBetween(min: Int, max: Int): Int = {
    min + Random.nextInt((max - min) + 1)
  }

  def generateString(len: Int): String = {
    Random.alphanumeric.take(len).mkString
  }

  def main(args: Array[String]): Unit = {
    val partitions = 1
    val dataper = 10000
    val name = "studentGrade_10000_1"
    val seed = Random.nextLong()
    Random.setSeed(seed)

    val sparkConf = new SparkConf()
    val datasets = Array(
      ("ds1", "src/resources/dataStudentGrade")
    )
    sparkConf.setMaster("local[6]")
    sparkConf.setAppName("DataGen: StudentGrade")

    println(
      s"""
         |partitions: $partitions
         |records: $dataper
         |seed: $seed
         |""".stripMargin
    )

    datasets.foreach { case (_, "src/resources/dataStudentGrade") =>
      SparkContext.getOrCreate(sparkConf).parallelize(Seq[Int](), partitions).mapPartitions { _ =>
        (1 to dataper).map { _ =>
          // CS050,61
          val course = s"CS${randIntBetween(100, 700)}"
          val score = randIntBetween(0, 1000)
          s"""$course,$score"""
        }.iterator
      }.saveAsTextFile("src/resources/dataStudentGrade")
    }
  }
}
