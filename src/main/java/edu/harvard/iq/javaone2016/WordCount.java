package edu.harvard.iq.javaone2016;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import static org.apache.spark.SparkContext.getOrCreate;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;


/**
 *
 * @author ellenk
 */

/*
scp /Users/ellenk/src/JavaOne2016/target/JavaOne2016-1.0-SNAPSHOT.jar root@consilience-build.iq.harvard.edu:/root/javaone
/Applications/spark-2.0.0-bin-hadoop2.7/bin/spark-submit --class edu.harvard.iq.javaone2016.WordCount --master spark://Ellens-MacBook-Pro-2.local:7077 --conf "spark.sql.shuffle.partitions=8" --verbose  /Users/ellenk/src/JavaOne2016/target/JavaOne2016-1.0-SNAPSHOT.jar "/Users/ellenk/test/text_doc_root/Laut/docs"
/Applications/spark-2.0.0-bin-hadoop2.7/bin/spark-submit --class edu.harvard.iq.javaone2016.WordCount --master spark://Ellens-MacBook-Pro-2.local:7077  --verbose  /Users/ellenk/src/JavaOne2016/target/JavaOne2016-1.0-SNAPSHOT.jar 
/root/spark-2.0.0-bin-hadoop2.7/bin/spark-submit --class edu.harvard.iq.javaone2016.WordCount --master mesos://zk://consilience-m1p.cloudapp.net:2181/mesos --conf "spark.sql.shuffle.partitions=32" --verbose  /root/javaone/JavaOne2016-1.0-SNAPSHOT.jar   "/mnt/consilience-smb1/docs"
 */
public class WordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        String dir = null;
        
        if (args.length > 0) {
            dir = args[0];
        }
      
        String master = "local[2]";
        String runtimeMaster = System.getProperty("spark.master");
        if (runtimeMaster != null) {
            master = runtimeMaster;
        }

        WordCount example = new WordCount();
        Builder builder =SparkSession
                .builder()
                .appName("WordCount Dataset Example")
                .master(master); 
        
     
        SparkSession spark = 
                builder.getOrCreate();
        example.run(spark, dir);
    }

    public void run(SparkSession spark, String dir) {
        List<String> data = Arrays.asList(
                "Hi I heard about Spark",
                "JavaOne is a great conference",
                "I love San Francisco",
                "I love Spark logistic regression models",
                "There are other models available in Spark",
                "The Chinese fortune cookie was invented by a Japanese resident of San Francisco",
                "San Francisco cable cars are the only National Historical Monument that can move",
                "Irish coffee was perfected and popularized in San Francisco");

        Dataset<Row> sentencesDF;
        if (dir == null) {
            Dataset<String> test = spark.createDataset(data, Encoders.STRING());
            sentencesDF = test.toDF();
        } else {
            sentencesDF = spark.read().text(dir);
        }

        Dataset<String> words = sentencesDF.flatMap((Row r) -> {
            return Arrays.asList(SPACE.split(r.getAs("value"))).iterator();
        }, Encoders.STRING());

        // Count word frequency
        Dataset<Row> counts = words.groupBy("value").count();
        // Sort by frequency
        Dataset<Row> sorted = counts.sort("count");
   
        List<Row> collected = sorted.collectAsList();
        collected.forEach(System.out::println);

        spark.stop();
    }

}
