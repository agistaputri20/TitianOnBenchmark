package customers
import org.apache.spark.lineage.LineageContext
import org.apache.spark.{SparkConf, SparkContext}

object customers {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    var lineage = true
    var customers_data = "hdfs://scai01.cs.ucla.edu:9000/clash/datasets/WB/"
    var orders_data = "hdfs://scai01.cs.ucla.edu:9000/clash/datasets/WB/"
    if (args.size < 2) {
      customers_data = "src/resources/customers/customers"
      orders_data = "src/resources/customers/orders"
      conf.setMaster("local[1]")
      lineage = true
    } else {
      lineage = args(0).toBoolean
      customers_data += args(1)
      orders_data += args(2)
      conf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
    }
    conf.setAppName("Customers-" + lineage)

    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)

    lc.setCaptureLineage(true)

    val customers = lc.textFile(customers_data).map(_.split(","))
    val orders = lc.textFile(orders_data).map(_.split(","))
//      .map {
//        cols =>
//          (cols(0), cols(1), cols(2).toInt, cols(3))
//      }
//      .filter{
//        s =>
//          s._3 < 1234
//      }


    //  ---------------------------------------------------------------------------------------

    // sample data point customers:
    //  CustomerID	CustomerName	ContactName	Country
    //  1,Alfreds Futterkiste,Maria Anders,Germany
    // sample data point orders:
    //  OrderID	CustomerID	OrderDate
    //  10308,2,1996-09-18

    val o = orders
      .map {
        case Array(_, cid, date, iid) => (cid, (iid, date.toInt))
      }
      .filter{
        s=>
          s._1 == "520"
      }

    o.collect.foreach(println)
    o.saveAsTextFile("src/output/customers/programOutput")

    val c = customers
      .map {
        row =>
          (row(0), row(1))
      }

    val joined = c.join(o)
      .filter { case (_, (_, (_, date))) =>
        val this_year = 1641013200
        if (date > this_year)
          true
        else
          false
      }

    val grouped = joined.groupByKey()
    val numpur = grouped.mapValues { iter => iter.size }
    val thresh = numpur.filter(arg1 => arg1._2 >= 3)
    //    val top = thresh.sortBy(_._2, ascending = false).take(3)
    //    if (top.length < 3) {
    //      println("not enough data")
    //    }
    //    else {
    //      val rewards = top.map { case (id, num) => (id, 100.0f, s"$id has won ${"$"}100.0f") }
    //      rewards.foreach(println)
    //    }

    lc.setCaptureLineage(false)
    //data lineage
    var linRdd = o.getLineage()

    //track all wrong input
    linRdd = linRdd.goBackAll()
    println("This is lineage of wrong input")
    linRdd.show.saveAsTextFile("src/output/customers/titianOutput")

  }

}
