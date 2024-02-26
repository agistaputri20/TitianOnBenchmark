package commuteTypeFull
import org.apache.spark.lineage.LineageContext
import org.apache.spark.{SparkConf, SparkContext}

object commuteTypeFull {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    var lineage = true
    var logFile1 = "hdfs://scai01.cs.ucla.edu:9000/clash/datasets/WB/"
    var logFile2 = "hdfs://scai01.cs.ucla.edu:9000/clash/datasets/WB/"
    if (args.size < 2) {
      logFile1 = "src/resources/dataCommuteTypeFull/trips"
      logFile2 = "src/resources/dataCommuteTypeFull/locations"
      conf.setMaster("local[1]")
      lineage = true
    } else {
      lineage = args(0).toBoolean
      logFile1 += args(1)
      logFile2 += args(2)
      conf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
    }
    conf.setAppName("CommuteTypeFull-" + lineage)

    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)

    lc.setCaptureLineage(true)

    // Job
    val trips = lc.textFile(logFile1).map(s => s.split(","))
      .map { cols =>
        (cols(1), cols(3).toInt / cols(4).toInt)
      }

    val locations = lc.textFile(logFile2).map(s => s.filter(_ != '\"').split(","))
      .map { cols =>
        (cols(0), cols(3))
      }
      .filter {
      s => commuteTypeFull.wrongInput(s._2)
    }

    val joined = trips.join(locations)
    val mapped = joined
      .map { s =>
        // Checking if speed is < 25mi/hr
        val speed = s._2._1
        if (speed > 40) {
          ("car", speed)
        } else if (speed > 15) {
          ("public", speed)
        } else {
          ("onfoot", speed)
        }
      }

    val rbk = mapped
      .reduceByKey(add)

    locations.collect().foreach(println)
    locations.saveAsTextFile("src/output/commuteTypeFull/programOutput")

    lc.setCaptureLineage(false)

    //data lineage
    var linRdd = locations.getLineage()

    //track all wrong input
    linRdd = linRdd.goBackAll()
    println("This is lineage of the input")
    linRdd.show.saveAsTextFile("src/output/commuteTypeFull/titianOutput")

    sc.stop()
  }

  def wrongInput(location: String): Boolean = {
    location == "Carolina"
  }

  def add(a: Int, b: Int): Int = {
    a + b
  }

}
