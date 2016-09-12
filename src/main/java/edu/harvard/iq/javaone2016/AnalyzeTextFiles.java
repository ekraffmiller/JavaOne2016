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


import java.io.File;
import java.io.IOException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;

import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
/*
scp /Users/ellenk/src/JavaOne2016/target/JavaOne2016-1.0-SNAPSHOT.jar root@consilience-build.iq.harvard.edu:/root/javaone
/Applications/spark-2.0.0-bin-hadoop2.7/bin/spark-submit --class edu.harvard.iq.javaone2016.AnalyzeTextFiles --master spark://Ellens-MacBook-Pro-2.local:7077  --verbose  /Users/ellenk/src/JavaOne2016/target/JavaOne2016-1.0-SNAPSHOT.jar "/Users/ellenk/test/text_doc_root/Laut/docs" 4 
 /root/spark-2.0.0-bin-hadoop2.7/bin/spark-submit --class edu.harvard.iq.javaone2016.AnalyzeTextFiles --master mesos://zk://consilience-m1p.cloudapp.net:2181/mesos   --verbose  /root/javaone/JavaOne2016-1.0-SNAPSHOT.jar  "/mnt/consilience-smb1/docs" 4
*/

public class AnalyzeTextFiles {
   static boolean show = true;
   public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: AnalyzeTextFiles <dir> <k>");
            System.exit(1);
        }
        String master = "local";
     
        String runtimeMaster = System.getProperty("spark.master");
        if (runtimeMaster!=null) {
            master = runtimeMaster;
        }
        String inputFile = args[0];
        int k = Integer.parseInt(args[1]);
       
        AnalyzeTextFiles  example = new AnalyzeTextFiles();
       
        SparkSession spark = SparkSession
                .builder()
                .appName("Analyze Text Files Example")
                .master(master) 
                .getOrCreate();

        example.runKmeans(spark, inputFile, k);
    }
   
    public void runKmeans(SparkSession spark, String dir, int k)  {
       
       JavaRDD<Tuple2<String,String>> stringRDD = spark.sparkContext().wholeTextFiles(dir, 4).toJavaRDD();
          stringRDD.cache();
          System.out.println("stringRDD.getStorageLevel() == NONE:" + stringRDD.getStorageLevel().equals(StorageLevel.NONE()));
       
     System.out.println("partitions: "+stringRDD.partitions().size());
     System.out.println("items:"+stringRDD.count());
     
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
   
      Tokenizer tokenizer = new Tokenizer().setInputCol("filetext").setOutputCol("words");
    Dataset<Row> wordsData = tokenizer.transform(fileData);
    if (show) fileData.show();
    if (show) wordsData.show();


CountVectorizerModel cvModel = new CountVectorizer()
  .setInputCol("words")
  .setOutputCol("rawfeatures")
  .setVocabSize(2000)
  .setMinDF(2)
  .fit(wordsData);



    Dataset<Row> rawfeatures =  cvModel.transform(wordsData);
    if (show) rawfeatures.select("words","rawfeatures").show();
    
    IDF idf = new IDF().setInputCol("rawfeatures").setOutputCol("features");
    IDFModel idfModel = idf.fit(rawfeatures);

   // Transforms term frequency (TF) vectors to TF-IDF vectors.
    Dataset<Row> rescaledData = idfModel.transform(rawfeatures);
   
    
    // Run kmeans on features
    Dataset<Row> features = rescaledData.select("features");
     
        // Trains a k-means model
        KMeans kmeans = new KMeans()
                .setK(k);
        features.persist(StorageLevel.MEMORY_ONLY());
        System.out.println("features.toJavaRDD().getStorageLevel() == NONE:" + features.toJavaRDD().getStorageLevel().equals(StorageLevel.NONE()));
        KMeansModel model = kmeans.fit(features);
        try {
        model.write().overwrite().save("/tmp/spark-kmeans-model");
        } catch(IOException e) {
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
       features.foreach(row -> {
          
            // This output appears in the 
          System.out.println("prediction: " + model.predict(row.getAs(0)));
       
           
        });
    
  

    spark.stop();
  }
}
