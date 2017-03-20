
package edu.harvard.iq.javaone2016;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 *
 * @author ellenk
 */

/*
 Commands for running on Stand Alone Cluster and locally and on cloud instances:

/Applications/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --class edu.harvard.iq.javaone2016.WordCountRDD --master spark://Ellens-MacBook-Pro-2.local:7077  --verbose  /Users/ellenk/src/JavaOne2016/target/JavaOne2016-1.0-SNAPSHOT.jar "/Users/ellenk/test/text_doc_root/Laut/docs" 
/home/glassfish/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --class edu.harvard.iq.javaone2016.WordCountRDD --master spark://iqss-devoxx-m1.cloudapp.net:7077  --verbose  /tmp/javaone/JavaOne2016-1.0-SNAPSHOT.jar  
*/
public class WordCountRDD {
    
    static final Pattern SPACE = Pattern.compile(" ");
    static final List<String> TEST_DATA = Arrays.asList(
       "Hi I heard about Spark",
       "Devoxx US is a great conference",
       "I love San Francisco",
       "I love Spark logistic regression models",
       "There are other models available in Spark",
       "The Chinese fortune cookie was invented by a Japanese resident of San Francisco",
       "San Francisco cable cars are the only National Historical Monument that can move",
       "Irish coffee was perfected and popularized in San Francisco");
    
    
    public static void main(String[] args) {
       

        SparkSession spark = SparkSession
                .builder()
                .appName(" WordCount RDD Example ")
                .getOrCreate();

        // Get a JavaSparkContext so we can work with RDDs
        JavaSparkContext jSparkContext = new JavaSparkContext(spark.sparkContext());

        // Distribute data to cluster nodes   
        JavaRDD<String> sentences;
        if (args.length == 0) {
            sentences = jSparkContext.parallelize(TEST_DATA);
        } else {
            sentences = jSparkContext.textFile(args[0]);
        }

        // Convert sentences to words ( lamda executes on nodes)
        JavaRDD<String> words = sentences.flatMap((String s)
                -> Arrays.asList(SPACE.split(s.toLowerCase())).iterator());

        // Convert each word to a pair to prep for counting
        JavaPairRDD<String, Integer> ones = words.mapToPair((String s) -> new Tuple2<>(s, 1));

        // Count word frequency
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((Integer i1, Integer i2) -> i1 + i2);

        // flip the pair to make the frequency the key
        JavaPairRDD<Integer, String> intpairs = counts.mapToPair((Tuple2<String, Integer> tuple) -> {
            return new Tuple2<>(tuple._2, tuple._1);
        });

        // Order by frequency
        JavaPairRDD<Integer, String> sortedIntPairs = intpairs.sortByKey();
        sortedIntPairs.foreach((pair) ->  {System.out.println("distributed- "+pair._1 +": "+ pair._2);}); 
        // Call to collect() triggers DAG execution
        List<Tuple2<Integer, String>> output = sortedIntPairs.collect();

        output.forEach((tuple) -> {
            System.out.println(tuple._1() + ":  " + tuple._2());
        });

        spark.stop();

    }

}
