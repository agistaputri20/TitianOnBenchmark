package mapString
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.lineage.LineageContext

object mapString {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    var lineage = true
    var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/datasets/WB/"
    if (args.size < 2) {
      logFile = "src/resources/dataMapString"
      conf.setMaster("local[1]")
      lineage = true
    } else {
      lineage = args(0).toBoolean
      logFile += args(1)
      conf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
    }
    conf.setAppName("MapString-" + lineage + "-" + logFile)

    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)


    lc.setCaptureLineage(true)

    // Job
    val text = lc.textFile(logFile).map(_.split("\n"))

    val mapped = text.map { s =>
      s(0)
    }


    println("This is mapped")
    mapped.collect.foreach(println)
    mapped.saveAsTextFile("src/output/mapString/programOutput")

    lc.setCaptureLineage(false)

    //data lineage
    var linRdd = mapped.getLineage()

    //track all input
    linRdd = linRdd.goBackAll()
    println("This is lineage of these input")
    linRdd.show(true).saveAsTextFile("src/output/mapString/titianOutput")

    sc.stop()
  }

}
