package commuteType
import org.apache.spark.lineage.LineageContext
import org.apache.spark.{SparkConf, SparkContext}

object commuteType {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    var lineage = true
    var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/datasets/WB/"
    if (args.size < 2) {
      logFile = "src/resources/dataCommuteType"
      conf.setMaster("local[1]")
      lineage = true
    } else {
      lineage = args(0).toBoolean
      logFile += args(1)
      conf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
    }
    conf.setAppName("CommuteType-" + lineage + "-" + logFile)

    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)

    lc.setCaptureLineage(true)

    // Job
    val trips = lc.textFile(logFile).map { s =>
      val cols = s.split(",")
      (cols(1), Integer.parseInt(cols(3)) / Integer.parseInt(cols(4)))
    }

    val types = trips.map { s =>
      val speed = s._2
      if (speed > 40) {
        ("car", speed)
      } else if (speed > 15) {
        ("public", speed)
      } else {
        ("onfoot", speed)
      }
    }.map { case (a, b) => (a, b) }
      .filter(s => commuteType.failure(s._2))

    types.reduceByKey((a, b) => a + b)
      .collect()
      .foreach(println)

    types.collect.foreach(println)
    types.saveAsTextFile("src/output/commuteType/programOutput")

    lc.setCaptureLineage(false)

    //data lineage
    var linRdd = types.getLineage()

    //track all wrong input
    linRdd = linRdd.goBackAll()
    println("This is lineage of the input")
    linRdd.show.saveAsTextFile("src/output/commuteType/titianOutput")

    sc.stop()
  }

  def failure(speed: Int): Boolean = {
    speed > 100
  }
}
