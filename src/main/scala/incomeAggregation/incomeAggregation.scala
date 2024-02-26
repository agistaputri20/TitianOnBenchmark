package incomeAggregation

import insideCircle.insideCircle
import org.apache.spark.lineage.LineageContext
import org.apache.spark.{SparkConf, SparkContext}

object incomeAggregation {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    var lineage = true
    var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/datasets/WB/"
    if (args.size < 2) {
      logFile = "src/resources/dataIncomeAggregation"
      conf.setMaster("local[1]")
      lineage = true
    } else {
      lineage = args(0).toBoolean
      logFile += args(1)
      conf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
    }
    conf.setAppName("IncomeAggregation-" + lineage + "-" + logFile)

    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)


    lc.setCaptureLineage(true)

    // Job
    val data = lc.textFile(logFile).map(_.split(","))
      .map {
        cols =>
          (cols(0), cols(1).toInt, cols(2).toInt)
      }.filter { s =>
      s._1 == "90024"
    }.map {
      s =>
        if (s._2 >= 40 & s._2 <= 65) {
          ("40-65", (s._3, 1))
        } else if (s._2 >= 20 & s._2 < 40) {
          ("20-39", (s._3, 1))
        } else if (s._2 < 20) {
          ("0-19", (s._3, 1))
        } else {
          (">65", (s._3, 1))
        }
    }
    data
      .mapValues(x => (x._2, x._2.toDouble))
      .take(100).foreach(println)

    data.collect.foreach(println)
    data.saveAsTextFile("src/output/incomeAggregation/programOutput")

    lc.setCaptureLineage(false)

    //data lineage
    var linRdd = data.getLineage()

    //track all wrong input
    linRdd = linRdd.goBackAll()
    println("This is lineage of this mapped2")
    linRdd.show(true).saveAsTextFile("src/output/incomeAggregation/titianOutput")

    sc.stop()
  }

  def sum(x: (Int, Int), y: (Int, Int)): (Int, Int) = {
    (x._1 + y._1, x._2 + y._2)
  }

}
