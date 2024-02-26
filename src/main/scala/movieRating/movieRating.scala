package movieRating
import insideCircle.insideCircle
import org.apache.spark.lineage.LineageContext
import org.apache.spark.{SparkConf, SparkContext}

object movieRating {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    var lineage = true
    var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/datasets/WB/"
    if (args.size < 2) {
      logFile = "src/resources/dataMovieRating"
      conf.setMaster("local[1]")
      lineage = true
    } else {
      lineage = args(0).toBoolean
      logFile += args(1)
      conf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
    }
    conf.setAppName("MovieRating-" + lineage + "-" + logFile)

    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)


    lc.setCaptureLineage(true)

    // Job
    val data = lc.textFile(logFile).map(_.split(","))

    val mapped = data.map {
      r =>
        val movie_str = r(0)
        val ratings = r(1)
        (movie_str, ratings.toInt)
    }.filter(s => movieRating.failure(s._2))
      .map { case (a, b) => (a, b.asInstanceOf[Any]) } // Temporary fix

    mapped.reduceByKey(sum)
      .take(100)
      .foreach(println)


    println("This is mapped")
    mapped.collect.foreach(println)
    mapped.saveAsTextFile("src/output/movieRating/programOutput")

    lc.setCaptureLineage(false)

    //data lineage
    var linRdd = mapped.getLineage()

    //track all wrong input
    linRdd = linRdd.goBackAll()
    println("This is lineage of this mapped2")
    linRdd.show(true).saveAsTextFile("src/output/movieRating/titianOutput")

    sc.stop()
  }

  def failure(rating: Int): Boolean = {
    rating < 4
  }

  def sum(a: Any, b: Any): Int = {
    (a, b) match {
      case (x: Int, y: Int) => x + y
    }
  }

}
