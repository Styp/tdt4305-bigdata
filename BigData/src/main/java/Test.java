import com.google.common.base.CaseFormat;
import com.google.common.base.CharMatcher;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages;
import py4j.StringUtil;
import scala.Array;
import scala.Tuple1;
import scala.Tuple1$;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by martin on 27.03.17.
 */
public class Test {

    public static void main(String[] args) {

        System.out.println("Test");

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkJoins").setMaster("local"));

        JavaRDD<String> textFile = sc.textFile("input/listings_us.csv");

        JavaPairRDD<String, Integer> filteredSigns = textFile
                .flatMap(s -> Arrays.asList(s.split("\n")).iterator())
                .map( line -> {
                    String[] parts = line.split("\t");

                    //19 is our lucky number - description field
                    String tmpString = Arrays.asList(parts[19]).toString();
                    String tmpStringOnlyLetters = CharMatcher.is(' ')
                            .or(CharMatcher.javaLetter())
                            .retainFrom(tmpString).toLowerCase();

                    return tmpStringOnlyLetters;
                })
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(element -> new Tuple2<>(element, 1))
                .reduceByKey((a, b) -> a + b);

        JavaPairRDD<Integer, String> swappedPair = filteredSigns.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
                return item.swap();
            }

        });

        swappedPair.sortByKey(false,1).saveAsTextFile("output/ordered_result.txt");

        filteredSigns.saveAsTextFile("output/output.txt");


    }

}
