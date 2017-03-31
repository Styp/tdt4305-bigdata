import com.google.common.base.CharMatcher;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.*;

import java.io.File;
import java.io.IOException;
import java.lang.Boolean;
import java.lang.Double;
import java.util.*;
public class AlternativeListings {


    public static void main(String[] args) {
        cleanDirectory();

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkJoins").setMaster("local"));

        JavaRDD<String> listingsFile = sc.textFile("input/listings_us.csv");
        JavaRDD<String> calendarFile = sc.textFile("input/calendar_us.csv");

        JavaRDD<ListingsObj> listingsObjsRDD = listingsFile
                .flatMap(s -> Arrays.asList(s.split("\n")).iterator())
                .map((line) -> {
                    ListingsObj listingsObj = new ListingsObj(line);



                    return listingsObj;
                })
                .filter(listingsObj -> !listingsObj.isHeader());

        int id = 12607303;
        String room_type = listingsObjsRDD.filter(x -> x.listingsId == id).first().room_type;
        String date = "2017-05-31";
        int percentage = 10;
        double price = listingsObjsRDD.filter(x -> x.listingsId == id).first().price;
        double longitude = listingsObjsRDD.filter(x -> x.listingsId == id).first().longitude;
        double latitude = listingsObjsRDD.filter(x -> x.listingsId == id).first().latitude;
        double km = 20;
        //String[] amenities = listingsObjsRDD.filter(x -> x.listingsId == id).first().amenities;


        List<Integer> relevantListingIds = calendarFile
                .flatMap(s -> Arrays.asList(s.split("\n")).iterator())
                .map((line) -> {
                    String[] parts = line.split("\t");
                    CalendarObj calendarObj = new CalendarObj();
                    calendarObj.id = ParserHelper.integerParse(parts[0]);
                    calendarObj.date = parts[1];
                    calendarObj.availability = parts[2].equals("t");

                    return calendarObj;
                }).filter(x -> x.date.equals(date) && x.availability == true)
                .map(x -> x.id)
                .collect();
        //                .filter(x -> x._2().equals(date)).filter(x -> x._3().equals(true)).map(x -> x._1());

        JavaRDD<ListingsObj> filter = listingsObjsRDD.filter(x -> relevantListingIds.contains(x.listingsId))
                .filter(x -> x.room_type.equals(room_type))
                .filter(x -> x.price <= price * (1 + (percentage / 100)))
                .filter(x -> getDistance(longitude,x.longitude,latitude,x.latitude) < km);

        filter.coalesce(1).saveAsTextFile("output/test");

    }

    public static void cleanDirectory(){
        // Clean things up! :)
        try {
            FileUtils.deleteDirectory(new File("output"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static double getDistance(double long1,double long2, double lat1, double lat2) {
        double dlon = long2 - long1;
        double dlat = lat2 - lat1;
        double a = (Math.sin(dlat / 2) * Math.sin(dlat / 2)) + Math.cos(lat1) * Math.cos(lat2) * (Math.sin(dlon / 2) * Math.sin(dlon / 2));
        double c = 2 * Math.asin(Math.sqrt(a));
        int r = 6371;
        return c * r;
    }
}
