/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

        
        Dataset<Row> fileData = spark.read().option("header", true).option("inferSchema",true ).csv(csvFile);
        fileData.createOrReplaceTempView("publications");
        Dataset<Row> journals = spark.sql("select * from publications where ItemType='Journal Article' ");
        Tokenizer tokenizer = new Tokenizer().setInputCol("Abstract").setOutputCol("raw");
        Dataset<Row> rawData = tokenizer.transform(journals);
      
        StopWordsRemover remover = new StopWordsRemover()
        .setInputCol("raw")
        .setOutputCol("filtered"); 
       
        Dataset<Row> wordsData = remover.transform(rawData);
        wordsData.show();
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

          // Transforms term frequency (TF) vectors to TF-IDF vectors.
        IDF idf = new IDF().setInputCol("rawfeatures").setOutputCol("features");
        IDFModel idfModel = idf.fit(rawfeatures);
        Dataset<Row> rescaledData = idfModel.transform(rawfeatures);

        // Run kmeans on features
        Dataset<Row> features = rescaledData.select("features", "Title");

        // Trains a k-means model
        KMeans kmeans = new KMeans()
                .setK(k).setMaxIter(200).setTol(.00001);
         KMeansModel model = kmeans.fit(features);
      
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
                  System.out.println("clusterId: "+model.predict(r.getAs(0))+" - "+r.getAs(1));
              });
     

        
    }
}
