/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.harvard.iq.javaone2016;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 *
 * @author ellenk
 */
// /Applications/spark-1.5.0-bin-hadoop2.6/bin/spark-submit --class edu.harvard.iq.javaone2016.WordCountSpark15 --master spark://Ellens-MacBook-Pro.local:7077  --total-executor-cores 4 /Users/ellenk/src/JavaOne2016/target/JavaOne2016-1.0-SNAPSHOT.jar 

public class WordCountSpark15 {
    
   private static final Pattern SPACE = Pattern.compile(" ");    
    public static void main(String[] args) {
 
        String master = "local[2]";
        String runtimeMaster = System.getProperty("spark.master");
        System.out.println("runtimeMaster: "+runtimeMaster);
        if (runtimeMaster!=null) {
            master = runtimeMaster;
        }
      
        WordCountSpark15 example = new WordCountSpark15();
       SparkConf sparkConf = new SparkConf();
            sparkConf.setMaster(master); 
            SparkContext spark = new SparkContext();
        
       

        example.run(spark);
    }

    public void run(SparkContext spark) {
        List<String> data = Arrays.asList(
       "Hi I heard about Spark",
       "JavaOne is a great conference",
       "I love San Francisco!",
       "I love Spark logistic regression models!",
       "There are other models available in Spark",
       "San Francisco needs more vegan restaurants",
       "I heard there are some great vegan restaurants in Berkeley",
       "There are many Mexican restaurants too");
    
    JavaSparkContext jsc = new JavaSparkContext(spark);   
    JavaRDD<String> sentences = jsc.parallelize(data);
    System.out.println("partitions: "+ sentences.partitions().size());
 //   JavaRDD<String> words = sentences.flatMap((String s) -> Arrays.asList(SPACE.split(s)).iterator());
 
    JavaPairRDD<String, Integer> ones = sentences.mapToPair((String s) -> new Tuple2<String, Integer>(s, 1));

    JavaPairRDD<String, Integer> counts = ones.reduceByKey((Integer i1, Integer i2) -> i1 + i2);

    System.out.println("counts.toDebugString():" + counts.toDebugString());
    
    List<Tuple2<String, Integer>> output = counts.collect();
    output.stream().forEach((tuple) -> {
        System.out.println(tuple._1() + ":  " + tuple._2());
      }); 
    
   
    
    spark.stop();
    
    }
    
}
