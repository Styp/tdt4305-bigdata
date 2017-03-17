import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

val rawData = spark.read.option("delimiter", "\t").option("header", true).csv("/home/jonas/Downloads/listings_us.csv")
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
