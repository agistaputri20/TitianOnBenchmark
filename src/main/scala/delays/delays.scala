package delays

import org.apache.spark.lineage.LineageContext
import org.apache.spark.{SparkConf, SparkContext}

object delays {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster(if (args.length > 2) args(2) else "local[*]")
    conf.setAppName("Bus Delays")
    val sc = SparkContext.getOrCreate(conf)
    val lc = new LineageContext(sc)

    lc.setCaptureLineage(true)

    //<id>,<departure_time>,<advertised_departure>
    val station1 = lc.textFile("src/resources/dataStation1")
      .map(_.split(','))
      .filter(r => delays.failure(r(1).toInt, r(2).toInt))
      .map(r => (r(0), (r(1), r(2), r(3))))


    //<id>,<arrival_time>,<advertised_arrival>
    val station2 = lc.textFile("src/resources/dataStation2")
      .map(_.split(','))
      .filter(r => delays.failure(r(1).toInt, r(2).toInt))
      .map(r => (r(0), (r(1), r(2), r(3))))


    val joined = station1
      .join(station2)


    val mapped = joined
      .map { case (_, ((dep, adep, rid), (arr, aarr, _))) => (buckets((arr.toInt - aarr.toInt) - (dep.toInt - adep.toInt)), rid) } //bug idea, don't cater for early arrivals
    val grouped = mapped.groupByKey()
    val filtered = grouped
      .filter(_._1 > 2) // filter delays more than an hour
      .flatMap(_._2)
      .map((_, 1))

    val reduced = filtered
      .reduceByKey { case (a, b) => a + b }

//    reduced
//      .collect()
//      .foreach(println)


    station1.collect.foreach(println)
    station1.saveAsTextFile("src/output/delays/programOutput")

//    station2.collect.foreach(println)

    lc.setCaptureLineage(false)

    //data lineage
    var linRdd = station1.getLineage()

//    var linRdd2 = station2.getLineage()

    //track all wrong input in station 1
    linRdd = linRdd.goBackAll()
    println("This is lineage of input on station 1")
    linRdd.show(true).saveAsTextFile("src/output/delays/titianOutput")

//    track all wrong input in station 2

//    linRdd2 = linRdd2.goBackAll()
//    println("This is lineage of input on station 2")
//    linRdd.show(true)


    sc.stop()


  }

  def buckets(v: Int): Int = {
    v / 1800 // groups of 30 min delays
  }

  def failure (a: Int, d: Int): Boolean = {
    a < d
  }

}
