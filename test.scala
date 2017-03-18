import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

//val rawData = spark.read.option("delimiter", "\t").option("header", true).csv("/home/jonas/Downloads/listings_us.csv")

val rawData = spark.read.option("delimiter", "\t").option("header", true).csv("/home/martin/Desktop/tdt4305-bigdata/data/listings_us.csv")

val sample = rawData.sample(false ,0.05, 7)
val booking = sample.select("price")

//val word = booking.first.mkString.splitAt(1)._2.toDouble

booking.foreach { value => value.first.mkString.splitAt(1)._2.toDouble }
booking.flatMap(value => value.split("$").map(value => (value._2)))

//val temp = rawData.select("city")
//val cities = temp.distinct
//cities.foreach { city => println(city)}
//cities.count


//rawData.columns.foreach{ column => println(column + "  " + rawData.select(column).distinct.count)}

booking.map(value => value.mkString.splitAt(1)._2).filter(_.toString.contains(","))
booking.map(value => value.mkString.splitAt(1)._2).foreach {x => if x.toString.contains(",") x.toString.split(",")}


// Solution:
val aggregatedAverage = booking.map(value => value.mkString.replaceAll(",","").splitAt(1)._2.toDouble).map(a => (a,1)).reduce((a,b) => (a._1+b._1,a._2+b._2));
val averagePerNight = aggreagatedAverage._1 / aggregatedAverage._2;

/** 3d
val nightsBooked = listings.select("reviews_per_month").filter(col("reviews_per_month").isNotNull).map(a => a.toString.replace("[","").replace("]","").toDouble).reduce(_+_) / 7 * 10 * 3 * 12
*/

/** 4a
val globalAvg = rawData.groupBy("host_id").count.select("count").map(value => value.mkString.toDouble).reduce(_+_) / rawData.groupBy("host_id").count.orderBy(desc("count")).count
*/

/** 4b
val percentage = rawData.groupBy("host_id").count.filter(col("count") > 1).count.toDouble / rawData.groupBy("host_id").count.count * 100
*/


val rich = calendar.filter(col("available").equalTo("t")).join(listings,$"listing_id" === $"id").groupBy("host_id","price").count.map(v => (v.get(0).asInstanceOf[String],v.get(1).asInstanceOf[String].replace("$","").replaceAll(",","").toDouble * v.get(2).asInstanceOf[Long])).groupBy("_1").sum("_2").orderBy(desc("sum(_2)"))
