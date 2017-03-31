import com.google.common.base.CharMatcher;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
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

    public static StartupParams loadParams(String[] args){
        StartupParams params = new StartupParams();

        if(args.length != 5){
            throw new RuntimeException("Number of Arguments wrong");
        }

        Integer listingId = Ints.tryParse(args[0]);
        if(listingId == null){
            throw new RuntimeException("ListingId is not a valid number");
        } else{
            params.listingId = listingId;
        }

        params.date = args[1];

        Double percentage = Double.parseDouble(args[2]);
        if(percentage == null){
            throw new RuntimeException("Percentage is not a valid number");
        } else{
            params.percentage = percentage;
        }

        Double kiloMeters = Double.parseDouble(args[3]);
        if(kiloMeters == null){
            throw new RuntimeException("Kilo meters is not a valid number");
        } else{
            params.kiloMeters = kiloMeters;
        }

        Integer topN = Integer.parseInt(args[4]);
        if(topN == null){
            throw new RuntimeException("Top N is not a valid number");
        } else{
            params.topN = topN;
        }
        return params;
    }


    public static void main(String[] args) {
        final StartupParams startupParams = loadParams(args);
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


        ListingsObj object = ListingsHelper.getObjectForListingsId(listingsObjsRDD, startupParams.listingId);


        List<Integer> relevantListingIds = calendarFile
                .flatMap(s -> Arrays.asList(s.split("\n")).iterator())
                .map((line) -> {
                    CalendarObj calendarObj = new CalendarObj(line);
                    return calendarObj;
                }).filter(x -> x.date.equals(startupParams.date) && x.availability == true)
                .map(x -> x.id)
                .collect();

        List<Tuple5> resultSet = listingsObjsRDD.filter(x -> relevantListingIds.contains(x.listingsId))
                .filter(x -> x.room_type.equals(object.room_type))
                .filter(x -> x.price <= object.price * (1 + (startupParams.percentage / 100)))
                .filter(x -> x.getDistance(object) < startupParams.kiloMeters)
                .map(x -> new Tuple5(x.listingsId, x.name, x.numberOfMatchingAmenities(object), x.getDistance(object), x.price))
                .sortBy(x -> x._3(), false, 1).take(startupParams.topN);

        try {
            FileWriter.write5("output.tsv", resultSet);
        } catch (IOException e) {
            throw new RuntimeException("Can't wirte to file: " + e);
        }
        //filteredRDD.rdd().saveAsTextFile("output/final");


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
