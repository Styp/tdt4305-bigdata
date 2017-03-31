import com.clearspring.analytics.util.Lists;
import com.google.common.base.CharMatcher;
import com.google.common.primitives.Ints;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

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

        String neighborhoodName = "Belltown";
        JavaRDD<String> neighborhoodRDD = sc.textFile("input/neighborhood_test.csv");
        List<Integer> listingInNeighborhood = neighborhoodRDD.flatMap(s -> Arrays.asList(s.split("\n")).iterator())
                .map((line) -> {
                    String[] parts = line.split("\t");

                    NeighborhoodObj neighborhoodObj = new NeighborhoodObj();
                    neighborhoodObj.name = parts[1];
                    neighborhoodObj.id = ParserHelper.integerParse(parts[0]);

                    return neighborhoodObj;
                }).filter(x -> x.name.equals(neighborhoodName)).map(x -> x.id).collect();

        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = sc.parallelize(Arrays.asList(allListings.filter(x -> listingInNeighborhood.contains(x.listingsId))
                .map(x -> x.description)
                .reduce((a, b) -> a.concat(b)).split(" ")))
                .mapToPair(x -> new Tuple2<>(x, 1))
                .reduceByKey((a, b) -> a + b);


        //System.out.println(reduce);
        stringIntegerJavaPairRDD.coalesce(1).saveAsTextFile("neighborhood");


    }

    private static void computeForListing(JavaRDD<ListingsObj> allListings, int listingsId) {

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
        JavaRDD<Tuple2<String, Double>> weight_tdf = sc.parallelizePairs(wordOccupancyInTuple).cartesian(idft_value)
                .filter(x -> x._1._1 == x._2._1)
                .map(x -> new Tuple2<>(x._1._1, x._1._2 * x._2._2));

        weight_tdf.coalesce(1).saveAsTextFile("output/weighted_tdf");

    }

    private static JavaRDD<ListingsObj> generateListingsRDDFromTextFile(JavaRDD<String> textFileRdd) {

        JavaRDD<ListingsObj> eachListing = textFileRdd
                .flatMap(s -> Arrays.asList(s.split("\n")).iterator())
                .map( (line) -> {
                    ListingsObj listingsObj = new ListingsObj(line);

                    //Handle description
                    String[] parts = line.split("\t");

                    //19 is our lucky number - description field


                    return listingsObj;
                })
                .filter((listingsObj -> !listingsObj.isHeader()));

         return eachListing;

    }

}
