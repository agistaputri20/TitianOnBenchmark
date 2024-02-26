package findSalary

import org.apache.spark.lineage.LineageContext
import org.apache.spark.{SparkConf, SparkContext}

object findSalary {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    var lineage = true
    var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/datasets/WB/"
    if (args.size < 2) {
      logFile = "src/resources/dataFindSalary"
      conf.setMaster("local[1]")
      lineage = true
    } else {
      lineage = args(0).toBoolean
      logFile += args(1)
      conf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
    }
    conf.setAppName("FindSalary-" + lineage + "-" + logFile)

    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)


    lc.setCaptureLineage(true)

    // Job
    val data = lc.textFile(logFile).map(_.split(","))
      .map(_ (0))
      .map {
        line =>
          if (line.substring(0, 1).equals("$")) {
            line.substring(1, 6).toInt
          } else {
            line.toInt
          }
      }
    val filtered = data.filter { r =>
      r >= 300
    }
    filtered.reduce { (a, b) =>
      val sum = a + b
      sum
    }

    filtered.collectWithId.foreach(println)
    filtered.saveAsTextFile("src/output/findSalary/programOutput")

    lc.setCaptureLineage(false)

    //data lineage
    var linRdd = filtered.getLineage()

    //track all wrong input
    linRdd = linRdd.goBackAll()
    println("This is lineage of this input")
    linRdd.show(true).saveAsTextFile("src/output/findSalary/titianOutput")

    sc.stop()
  }

}
