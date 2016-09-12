/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
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
scp /Users/ellenk/src/JavaOne2016/target/JavaOne2016-1.0-SNAPSHOT.jar root@consilience-build.iq.harvard.edu:/root/javaone
/Applications/spark-2.0.0-bin-hadoop2.7/bin/spark-submit --class edu.harvard.iq.javaone2016.WordCount --master spark://Ellens-MacBook-Pro.local:7077  --verbose  /Users/ellenk/src/JavaOne2016/target/JavaOne2016-1.0-SNAPSHOT.jar 
/root/spark-2.0.0-bin-hadoop2.7/bin/spark-submit --class edu.harvard.iq.javaone2016.WordCount --master mesos://zk://consilience-m1p.cloudapp.net:2181/mesos  --verbose  /root/javaone/JavaOne2016-1.0-SNAPSHOT.jar  
*/
public class WordCountFilter {
    
   private static final Pattern SPACE = Pattern.compile(" ");    
    public static void main(String[] args) {
 
        String master = "local[4]";
        String runtimeMaster = System.getProperty("spark.master");
        if (runtimeMaster!=null) {
            master = runtimeMaster;
        }
      
        WordCountFilter example = new WordCountFilter();
       
        SparkSession spark = SparkSession
                .builder()
                .appName("Sorted WordCount Example with cache")
                .master(master) 
                .getOrCreate();

        example.run(spark);
    }

    public void run(SparkSession spark) {
        List<String> data = Arrays.asList(
       "Hi I heard about Spark",
       "JavaOne is a great conference",
       "I love San Francisco!",
       "I love Spark logistic regression models!",
       "There are other models available in Spark",
       "The Chinese fortune cookie was invented by a Japanese resident of San Francisco",
       "San Francisco cable cars are the only National Historical Monument that can move",
       "Irish coffee was perfected and popularized in San Francisco");
    
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());   
    JavaRDD<String> sentences = jsc.parallelize(data);
    System.out.println("partitions: "+ sentences.partitions().size());
    JavaRDD<String> words = sentences.flatMap((String s) -> Arrays.asList(SPACE.split(s.toLowerCase())).iterator());
    words.cache();
    long filter1Count = words.filter((String s) -> {return s.contains("!");}).count();
    long filter2Count = words.filter((String s) -> {return s.contains("c");}).count();
    System.out.println("filter1: "+filter1Count+", filter2: "+filter2Count);
    JavaPairRDD<String, Integer> ones = words.mapToPair((String s) -> new Tuple2<String, Integer>(s, 1));

    JavaPairRDD<String, Integer> counts = ones.reduceByKey((Integer i1, Integer i2) -> i1 + i2);
    JavaPairRDD<Integer, String> intpairs = counts.mapToPair((Tuple2<String,Integer> tuple )->  {return  new Tuple2<Integer,String>(tuple._2,tuple._1);});
    JavaPairRDD<Integer, String> sortedIntPairs = intpairs.sortByKey(false);
 
 
    System.out.println("sorted.toDebugString():" + sortedIntPairs.toDebugString());
    
    List<Tuple2<Integer, String>> output = sortedIntPairs.collect();
    
    
    output.forEach((tuple) -> {
        System.out.println(tuple._1() + ":  " + tuple._2());
      }); 
    
   
    
    spark.stop();
    
    }
    
}
