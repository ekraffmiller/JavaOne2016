

package edu.harvard.iq.javaone2016;
  

import java.util.List;  
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;

import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
/*
scp /Users/ellenk/src/JavaOne2016/target/JavaOne2016-1.0-SNAPSHOT.jar root@consilience-build.iq.harvard.edu:/root/javaone
/Applications/spark-2.0.0-bin-hadoop2.7/bin/spark-submit --class edu.harvard.iq.javaone2016.KMeansCSV --master spark://Ellens-MacBook-Pro-2.local:7077 --conf "spark.sql.shuffle.partitions=8"  --verbose  /Users/ellenk/src/JavaOne2016/target/JavaOne2016-1.0-SNAPSHOT.jar "/Users/ellenk/src/Text-Clustering/src/BibTexImport/metadatabibtexAbstract.csv" 4 
 /root/spark-2.0.0-bin-hadoop2.7/bin/spark-submit --class edu.harvard.iq.javaone2016.KMeansExample --master mesos://zk://consilience-m1p.cloudapp.net:2181/mesos   --verbose  /root/javaone/JavaOne2016-1.0-SNAPSHOT.jar  "/mnt/consilience-smb1/docs" 4
*/

public class KMeansCSV {
   static boolean show = true;
   public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: KMeansExample <file> <k>");
            System.exit(1);
        }
        String master = "local";
     
        String runtimeMaster = System.getProperty("spark.master");
        if (runtimeMaster!=null) {
            master = runtimeMaster;
        }
        String inputFile = args[0];
        int k = Integer.parseInt(args[1]);
       
        KMeansCSV  example = new KMeansCSV();
       
        SparkSession spark = SparkSession
                .builder()
                .appName("Kmeans Example")
                .master(master) 
                .getOrCreate();

        example.runKmeans(spark, inputFile, k);
        spark.stop();
    }
   
    public void runKmeans(SparkSession spark, String csvFile, int k) {

        // Read data from CSV file into Dataframe
        Dataset<Row> fileData = spark.read().option("header", true).option("inferSchema",true ).csv(csvFile);
        
        // Create a table view and run a sql query to filter the data
        fileData.createOrReplaceTempView("publications");
        Dataset<Row> journals = spark.sql("select * from publications where ItemType='Journal Article' ");
        
        // Split the "Abstract" column in to words
        Tokenizer tokenizer = new Tokenizer().setInputCol("Abstract").setOutputCol("raw");
        Dataset<Row> rawData = tokenizer.transform(journals);
        rawData.show();
        // Remove stop words from the raw list of words
        StopWordsRemover remover = new StopWordsRemover()
        .setInputCol("raw")
        .setOutputCol("filtered");       
        Dataset<Row> wordsData = remover.transform(rawData);
        
        // Show current contents of DF (20 rows)
        wordsData.show();
        
        // Create a feature vector from the words
        CountVectorizerModel cvModel = new CountVectorizer()
                .setInputCol("filtered")
                .setOutputCol("rawfeatures")
                .setVocabSize(10000)
                .setMinDF(2)
                .fit(wordsData);
        Dataset<Row> rawfeatures = cvModel.transform(wordsData);
        

        // Transforms term frequency (TF) vectors to TF-IDF vectors.
        IDF idf = new IDF().setInputCol("rawfeatures").setOutputCol("features");
        IDFModel idfModel = idf.fit(rawfeatures);
        Dataset<Row> rescaledData = idfModel.transform(rawfeatures);
        

        // Select a subset of columns for clustering
        Dataset<Row> features = rescaledData.select("features", "Title");

        // Train a k-means model on features
        KMeans kmeans = new KMeans()
                .setK(k).setMaxIter(200).setTol(.00001);
         KMeansModel model = kmeans.fit(features);
      
       
      // Collect features data back to driver 
      List<Row> resultList =features.collectAsList();
   
      // Predict a cluster assignment for each feature vector
      resultList.forEach((Row r) -> {
                  System.out.println("clusterId: "+model.predict(r.getAs(0))+" - "+r.getAs(1));
              });
     

        
    }
}
