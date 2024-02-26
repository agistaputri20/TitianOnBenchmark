package numberSeries

import org.apache.spark.lineage.LineageContext
import org.apache.spark.{SparkConf, SparkContext}

object numberSeries {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    var lineage = true
    var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/datasets/WB/"
    if (args.size < 2) {
      logFile = "src/resources/dataNumberSeries"
      conf.setMaster("local[1]")
      lineage = true
    } else {
      lineage = args(0).toBoolean
      logFile += args(1)
      conf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
    }
    conf.setAppName("NumberSeries-" + lineage + "-" + logFile)

    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)


    lc.setCaptureLineage(true)

    // Job
    val data = lc.textFile(logFile)
      .map(_.split(","))
      .map {
        s => s(1).toInt
      }

    val mapped = data.map
    { l =>
      var dis = 1
      var tmp = l
      if (l <= 0) {
        dis = 0
      } else {
        while (tmp != 1 && dis < 30) {
          if (tmp % 2 == 0) {
            tmp = (tmp / 2).toInt
          } else {
            tmp = tmp * 3 + 1
          }
          dis = dis + 1
        }
      }
      (l, dis)
    }.filter({
      case (l, m) =>
        m.!=(25)

    })


    println("This is mapped")
    mapped.collect.foreach(println)
    mapped.saveAsTextFile("src/output/numberSeries/programOutput")

    lc.setCaptureLineage(false)

    //data lineage
    var linRdd = mapped.getLineage()

    //track all wrong input
    linRdd = linRdd.goBackAll()
    println("This is lineage of this wrong input")
    linRdd.show(true).saveAsTextFile("src/output/numberSeries/titianOutput")

    sc.stop()
  }

}
