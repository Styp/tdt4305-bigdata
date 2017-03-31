import com.google.common.base.CharMatcher;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Array;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.*;
public class AlternativeListings {


    public static void main(String[] args) {
        cleanDirectory();

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkJoins").setMaster("local"));

        JavaRDD<String> textFile = sc.textFile("input/listings_us.csv");

        JavaRDD<ListingsObj> eachListing = textFile
                .flatMap(s -> Arrays.asList(s.split("\n")).iterator())
                .map( (line) -> {
                    ListingsObj listingsObj = new ListingsObj();


                    String[] parts = line.split("\t");

                    //Set fields

                    listingsObj.price = integerParse(parts[65].replaceAll("$","").replaceAll(",",""));
                    listingsObj.room_type = parts[81];
                    listingsObj.longitude = doubleParse(parts[54]);
                    listingsObj.latitude = doubleParse(parts[51]);

                    try {
                        listingsObj.listingsId = Integer.parseInt(parts[43]);
                    } catch(java.lang.NumberFormatException e) {
                        listingsObj.listingsId = -1;
                    }

                    return listingsObj;
                })
                .filter((listingsObj -> !listingsObj.isHeader()));

        eachListing.saveAsTextFile("output/test");

    }

    public static int integerParse(String s) {
        try {
            int i = Integer.parseInt(s);
            return i;
        } catch(java.lang.Exception e) {return -1;}
    }

    public static double doubleParse(String s) {
        try {
            double i = Double.parseDouble(s);
            return i;
        } catch(java.lang.Exception e) {return -1.0;}
    }

    public static void cleanDirectory(){
        // Clean things up! :)
        try {
            FileUtils.deleteDirectory(new File("output"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
