import com.google.common.base.CharMatcher;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.*;


public class tf_idf {

    public StartupParams assertArgs(String[] args){
        StartupParams params = new StartupParams();

        if(args.length != 3){
            throw new RuntimeException("Number of Arguments wrong");
        }

        String filePath = args[0] + "listings_us.csv";
        File file = new File(filePath);
        if (file.canRead() == false){
            throw new RuntimeException("Listings_csv.us - could not be found!");
        }
        params.filePath = filePath;

        String runMode = args[1];
        if(runMode == "-l"){

        } else if(runMode == "-n"){

        } else{
            throw new RuntimeException("Runtime Mode Unknown - Parameter should be -l / -n!");
        }

        return params;

    }

    public static void cleanDirectory(){
        // Clean things up! :)
        try {
            FileUtils.deleteDirectory(new File("output"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        assertArgs(args);
        cleanDirectory();

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkJoins").setMaster("local"));

        JavaRDD<String> textFile = sc.textFile("input/listings_us.csv");

        JavaRDD<ListingsObj> eachListing = textFile
                .flatMap(s -> Arrays.asList(s.split("\n")).iterator())
                .map( (line) -> {
                ListingsObj listingsObj = new ListingsObj();

                //Handle description
                String[] parts = line.split("\t");

                //19 is our lucky number - description field
                String tmpString = Arrays.asList(parts[19]).toString();
                String tmpStringOnlyLetters = CharMatcher.is(' ')
                        .or(CharMatcher.javaLetter())
                        .retainFrom(tmpString).toLowerCase();
                listingsObj.description = tmpStringOnlyLetters;

                try {
                    listingsObj.listingsId = Integer.parseInt(parts[43]);
                } catch(java.lang.NumberFormatException e) {
                    listingsObj.listingsId = -1;
                }

                return listingsObj;
                })
                .filter((listingsObj -> !listingsObj.isHeader()));

        eachListing.saveAsTextFile("output/output.txt");

        Double totalDocumentCount = eachListing.mapToDouble(e -> 1).reduce((x, y) -> x+y);
        System.out.println("Total Object count: " + totalDocumentCount);


        int ourListingId = 3254762;
        ListingsObj ourObject = eachListing
                .filter(listingsObj -> listingsObj.listingsId == ourListingId).first();


        JavaRDD<String> eachWord = sc.parallelize((Lists.newArrayList(ourObject.getTermFrequency().keySet())));
        JavaPairRDD<String, Double> idft_value = eachWord.cartesian(eachListing).filter(x -> x._2.getTermFrequency().keySet().contains(x._1))
                .mapToPair(obj -> new Tuple2<>(obj._1, 1)).reduceByKey((a,b) -> a + b).mapToPair(x -> new Tuple2<>(x._1, totalDocumentCount / x._2));

        idft_value.coalesce(1).saveAsTextFile("output/output_string_long.txt");


        List<Tuple2<String, Double>> wordOccupancyInTuple = new ArrayList<>();
        for(HashMap.Entry<String, Double> entry : ourObject.getWeightedTermFrequency().entrySet()) {
            wordOccupancyInTuple.add(new Tuple2<>(entry.getKey(), entry.getValue()));
        }
        JavaRDD<Tuple2<String, Double>> weight_tdf = sc.parallelizePairs(wordOccupancyInTuple).cartesian(idft_value)
                .filter(x -> x._1._1 == x._2._1)
                .map(x -> new Tuple2<>(x._1._1, x._1._2 * x._2._2));

        weight_tdf.coalesce(1).saveAsTextFile("output/weighted_tdf");
    }

}
