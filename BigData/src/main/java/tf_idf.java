import com.clearspring.analytics.util.Lists;
import com.google.common.base.CharMatcher;
import com.google.common.primitives.Ints;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.io.File;
import java.io.IOException;
import java.util.*;


public class tf_idf {

    public static JavaSparkContext sc;

    public static StartupParams loadParams(String[] args){
        StartupParams params = new StartupParams();

        if(args.length != 3){
            throw new RuntimeException("Number of Arguments wrong");
        }

        String filePath = args[0] + "listings_us.csv";
        File file = new File(filePath);
        if (file.canRead() == false){
            throw new RuntimeException("listings_csv.us - could not be found! " + filePath);
        }
        params.filePath = filePath;

        String runMode = args[1];
        if(runMode.equals("-l")){
            params.runMode = StartupParams.Mode.LISTING;

            Integer listingId = Ints.tryParse(args[2]);
            if(listingId == null){
                throw new RuntimeException("ListingId is not a valid number");
            } else{
                params.listingId = listingId;
            }

        } else if(runMode.equals("-n")){
            params.runMode = StartupParams.Mode.NEIGHBOURHOOD;

            String neighborhood = args[2];
            params.neighborhood = neighborhood;
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

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        final StartupParams startupParams = loadParams(args);
        cleanDirectory();

        sc = new JavaSparkContext(new SparkConf().setAppName("SparkJoins").setMaster("local"));

        JavaRDD<String> textFileRDD = sc.textFile(startupParams.filePath);
        JavaRDD<ListingsObj> allListings = generateListingsRDDFromTextFile(textFileRDD);

        switch (startupParams.runMode){
            case LISTING:
                computeForListing(allListings, startupParams.listingId);
                break;

            case NEIGHBOURHOOD:
                computerForNeighborhood(allListings, startupParams.neighborhood);
                break;
        }

    }

    private static void computerForNeighborhood(JavaRDD<ListingsObj> allListings, String neighborhood) {

        JavaRDD<String> neighborhoodRDD = sc.textFile("input/listings_ids_with_neighborhoods.tsv");

        JavaRDD<NeighborhoodObj> neighborhoodObjJavaRDD = neighborhoodRDD.flatMap(s -> Arrays.asList(s.split("\n")).iterator())
                .map((line) -> {
                    NeighborhoodObj neighborhoodObj = new NeighborhoodObj(line);
                    return neighborhoodObj;
                });

        long numberOfNeighborHoods = neighborhoodObjJavaRDD.groupBy(x -> x.name).count();

        List<Integer> listingInNeighborhood = neighborhoodObjJavaRDD.filter(x -> x.name.equals(neighborhood)).map(x -> x.id).collect();

        if(listingInNeighborhood.size() == 0){
            throw new RuntimeException("No Objects found for Neighboorhood: " + neighborhood);
        }

        String[] allWordsCollected = allListings.filter(x -> listingInNeighborhood.contains(x.listingsId))
                .map(x -> x.description)
                .reduce((a, b) -> a.concat(b)).split(" ");

        JavaRDD<String> eachWordInNeighborhood = sc.parallelize(Arrays.asList(allWordsCollected));

        Double totalNumberOfWords = eachWordInNeighborhood.mapToDouble(e -> 1).reduce((x, y) -> x + y);

        JavaPairRDD<String, Double> tftd_value = eachWordInNeighborhood.mapToPair(x -> new Tuple2<>(x, 1))
                .reduceByKey((a, b) -> a + b)
                .mapToPair(x -> new Tuple2<>(x._1, x._2 / totalNumberOfWords));


        JavaPairRDD<String, Set<String>> stringSetJavaPairRDD = allListings.cartesian(neighborhoodObjJavaRDD).filter(x -> x._1().listingsId == x._2.id)
                .mapToPair(x -> new Tuple2<>(x._2.name, x._1.getDescriptionAsSet())).reduceByKey((a, b) -> {
                    Set<String> tmpSet = new HashSet<>();
                    tmpSet.addAll(a);
                    tmpSet.addAll(b);
                    return tmpSet;
                });

        stringSetJavaPairRDD.coalesce(1).saveAsTextFile("output");
    }

    private static void computeForListing(JavaRDD<ListingsObj> allListings, int listingsId) {

        // input/ -l 3254762

        ListingsObj ourObject = ListingsHelper.getObjectForListingsId(allListings, listingsId);

        Double totalDocumentCount = allListings.mapToDouble(e -> 1).reduce((x, y) -> x+y);

        JavaRDD<String> eachWord = sc.parallelize((Lists.newArrayList(ourObject.getTermFrequency().keySet())));
        JavaPairRDD<String, Double> idft_value = eachWord.cartesian(allListings).filter(x -> x._2.getTermFrequency().keySet().contains(x._1))
                .mapToPair(obj -> new Tuple2<>(obj._1, 1))
                .reduceByKey((a,b) -> a + b)
                .mapToPair(x -> new Tuple2<>(x._1, totalDocumentCount / x._2));

        List<Tuple2<String, Double>> wordOccupancyInTuple = new ArrayList<>();
        for(HashMap.Entry<String, Double> entry : ourObject.getWeightedTermFrequency().entrySet()) {
            wordOccupancyInTuple.add(new Tuple2<>(entry.getKey(), entry.getValue()));
        }

        List<Tuple2> resultSet = sc.parallelizePairs(wordOccupancyInTuple).cartesian(idft_value)
                .filter(x -> x._1._1.equals(x._2._1))
                .map(x -> new Tuple2(x._1._1, Double.toString(x._1._2 * x._2._2))).sortBy(x -> x._2, false, 1).take(100);

        try {
            FileWriter.write2("output.tsv", resultSet);
        } catch (IOException e) {
            throw new RuntimeException("Can't write to file: " + e);
        }

    }

    private static JavaRDD<ListingsObj> generateListingsRDDFromTextFile(JavaRDD<String> textFileRdd) {

        JavaRDD<ListingsObj> eachListing = textFileRdd
                .flatMap(s -> Arrays.asList(s.split("\n")).iterator())
                .map( (line) -> {
                    ListingsObj listingsObj = new ListingsObj(line);
                    return listingsObj;
                })
                .filter((listingsObj -> !listingsObj.isHeader()));
         return eachListing;

    }

}
