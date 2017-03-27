
package edu.harvard.iq.javaone2016;
  

import java.io.File;
import java.io.IOException;
import java.util.List;  
import org.apache.spark.api.java.JavaRDD;
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
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
/*
scp /Users/ellenk/src/JavaOne2016/target/JavaOne2016-1.0-SNAPSHOT.jar root@consilience-build.iq.harvard.edu:/root/javaone
/Applications/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --class edu.harvard.iq.javaone2016.KMeansExample --master spark://Ellens-MacBook-Pro-2.local:7077 --conf "spark.sql.shuffle.partitions=8"  --verbose  /Users/ellenk/src/JavaOne2016/target/JavaOne2016-1.0-SNAPSHOT.jar "/Users/ellenk/test/text_doc_root/Laut/docs" 4 
 /root/spark-2.0.0-bin-hadoop2.7/bin/spark-submit --class edu.harvard.iq.javaone2016.KMeansExample --master mesos://zk://consilience-m1p.cloudapp.net:2181/mesos   --verbose  /root/javaone/JavaOne2016-1.0-SNAPSHOT.jar  "/mnt/consilience-smb1/docs" 4
*/

public class KMeansExample {
   static boolean show = true;
   public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: KMeansExample <dir> <k>");
            System.exit(1);
        }
        String master = "local";
     
        String runtimeMaster = System.getProperty("spark.master");
        if (runtimeMaster!=null) {
            master = runtimeMaster;
        }
        String inputFile = args[0];
        int k = Integer.parseInt(args[1]);
       
        KMeansExample  example = new KMeansExample();
       
        SparkSession spark = SparkSession
                .builder()
                .appName("Kmeans Example")
                .master(master) 
                .getOrCreate();

        example.runKmeans(spark, inputFile, k);
        spark.stop();
    }
   
    public void runKmeans(SparkSession spark, String dir, int k) {

        JavaRDD<Tuple2<String, String>> stringRDD = spark.sparkContext().wholeTextFiles(dir, 4).toJavaRDD();
        stringRDD.cache();

     
        JavaRDD<Row> rowRDD = stringRDD.map(doc -> {
            Object[] rowData = new Object[2];
            File f = new File(doc._1);
            rowData[0] = f.getName();
            rowData[1] = doc._2.trim();
            return new GenericRow(rowData);
        });

        StructType schema = new StructType(new StructField[]{
            new StructField("filename", DataTypes.StringType, false, Metadata.empty()),
            new StructField("filetext", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> fileData = spark.createDataFrame(rowRDD, schema);

        Tokenizer tokenizer = new Tokenizer().setInputCol("filetext").setOutputCol("raw");
        Dataset<Row> rawData = tokenizer.transform(fileData);
      
        StopWordsRemover remover = new StopWordsRemover()
        .setInputCol("raw")
        .setOutputCol("filtered"); 
       
        Dataset<Row> wordsData = remover.transform(rawData);
        
        CountVectorizerModel cvModel = new CountVectorizer()
                .setInputCol("filtered")
                .setOutputCol("rawfeatures")
                .setVocabSize(10000)
                .setMinDF(2)
                .fit(wordsData);

        Dataset<Row> rawfeatures = cvModel.transform(wordsData);
        if (show) {
            rawfeatures.select("filtered", "rawfeatures").show();
        }

        IDF idf = new IDF().setInputCol("rawfeatures").setOutputCol("features");
        IDFModel idfModel = idf.fit(rawfeatures);

        // Transforms term frequency (TF) vectors to TF-IDF vectors.
        Dataset<Row> rescaledData = idfModel.transform(rawfeatures);

        // Run kmeans on features
        Dataset<Row> features = rescaledData.select("features", "filename");

        // Trains a k-means model
        KMeans kmeans = new KMeans()
                .setK(k);
         KMeansModel model = kmeans.fit(features);
        try {
            model.write().overwrite().save("/tmp/spark-kmeans-model");
        } catch (IOException e) {
            System.out.println("error saving model file");
        }
        /*
        
        // Shows the result
        Vector[] centers = model.clusterCenters();
       
        System.out.println("Cluster Centers: ");
        for (Vector center : centers) {
            System.out.println(center);
        }
         */
        
      List<Row> resultList =features.collectAsList();
   
      resultList.forEach((Row r) -> {
                  System.out.println("file: "+r.getAs(1)+"clusterId: "+model.predict(r.getAs(0)));
              });
      
     

        
    }
}
