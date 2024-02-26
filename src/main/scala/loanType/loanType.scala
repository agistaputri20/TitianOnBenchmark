package loanType
import org.apache.spark.lineage.LineageContext
import org.apache.spark.{SparkConf, SparkContext}

object loanType {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    var lineage = true
    var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/datasets/WB/"
    if (args.size < 2) {
      logFile = "src/resources/dataLoanType"
      conf.setMaster("local[1]")
      lineage = true
    } else {
      lineage = args(0).toBoolean
      logFile += args(1)
      conf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
    }
    conf.setAppName("LoanType-" + lineage + "-" + logFile)

    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)


    lc.setCaptureLineage(true)

    // Job
    val data = lc.textFile(logFile).map(_.split(","))

    val mapped = data.map {
      a => (a(0).toFloat, a(1).toInt, a(2).toFloat, a(3))
    }.filter(s => loanType.failure(s._2))

    val mapped2 = mapped.map { s =>
      var a = s._1
      val upper = math.min(s._2, 30)
      for (i <- 1 to upper) {
        a = a * (1 + s._3)
      }
      (a, s._2, s._3, s._4)
    }

    println("This is mapped")
    mapped2.collect.foreach(println)
    mapped2.saveAsTextFile("src/output/loanType/programOutput")

    lc.setCaptureLineage(false)

    //data lineage
    var linRdd = mapped2.getLineage()

    //track all input
    linRdd = linRdd.goBackAll()
    println("This is lineage of the input")
    linRdd.show(true).saveAsTextFile("src/output/loanType/titianOutput")

    sc.stop()
  }

  def failure(years: Int): Boolean ={
    years > 30
  }

}
