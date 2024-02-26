package studentGrade

import org.apache.spark.lineage.LineageContext
import org.apache.spark.{SparkConf, SparkContext}

object studentGrade {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    var lineage = true
    var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/datasets/WB/"
    if (args.size < 2) {
      logFile = "src/resources/dataStudentGrade"
      conf.setMaster("local[1]")
      lineage = true
    } else {
      lineage = args(0).toBoolean
      logFile += args(1)
      conf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
    }
    conf.setAppName("StudentGrade-" + lineage + "-" + logFile)

    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)


    lc.setCaptureLineage(true)

    // Job
    val data = lc.textFile(logFile)
      .map(_.split(","))
      .map { a =>
        val ret = (a(0), a(1).toInt)
        ret
      }

    val rdd = data.filter(s => studentGrade.failure(s._2))
      .map { a =>
      if (a._2 > 40) (a._1 + " Pass", 1) else (a._1 + " Fail", 1)
    }
      .map { case (a, b) => (a, b) }


    rdd.reduceByKey((a, b) => a + b)
      .take(100)
      .foreach(println)


    println("This is mapped")
    rdd.collect.foreach(println)
    rdd.saveAsTextFile("src/output/studentGrade/programOutput")

    lc.setCaptureLineage(false)

    //data lineage
    var linRdd = rdd.getLineage()

    //track all wrong input
    linRdd = linRdd.goBackAll()
    println("This is lineage of this wrong input")
    linRdd.show(true).saveAsTextFile("src/output/studentGrade/titianOutput")

    sc.stop()
  }

  def failure(grade: Int): Boolean = {
    grade > 100
  }
}
