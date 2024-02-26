package externalCall

import org.apache.spark.lineage.LineageContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.log10

object externalCall {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    var lineage = true
    var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/datasets/WB/"
    if (args.size < 2) {
      logFile = "src/resources/dataExternalCall"
      conf.setMaster("local[1]")
      lineage = true
    } else {
      lineage = args(0).toBoolean
      logFile += args(1)
      conf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
    }
    conf.setAppName("ExternalCall-" + lineage + "-" + logFile)

    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)


    lc.setCaptureLineage(true)

    // Job
    val mapped = lc.textFile(logFile).map(_.split("\\s"))
      .flatMap(s => s)
      .map { s =>
        (s, 1)
      }.map { case (a, b) => (a, b) }

    mapped.reduceByKey((a, b) => a + b)
      .filter { v =>
        val v1 = log10(v._2)
        v1 <= 1
      }


    println("This is mapped")
    mapped.collect.foreach(println)
    mapped.saveAsTextFile("src/output/externalCall/programOutput")

    lc.setCaptureLineage(false)

    //data lineage
    var linRdd = mapped.getLineage()

    //track all wrong input
    linRdd = linRdd.goBackAll()
    println("This is lineage of wrong input")
    linRdd.show(true).saveAsTextFile("src/output/externalCall/titianOutput")

    sc.stop()
  }


}
