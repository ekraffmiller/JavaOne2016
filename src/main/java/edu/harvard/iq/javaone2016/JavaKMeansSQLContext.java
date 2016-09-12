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

import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


/**
 * An example demonstrating a k-means clustering.
 * Run with
 * <pre>
 * bin/run-example ml.JavaSimpleParamsExample <file> <k>
 * </pre>
 */
public class JavaKMeansSQLContext {

 

  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("Usage: ml.JavaKMeansExample <file> <k>");
      System.exit(1);
    }
    String inputFile = args[0];
    int k = Integer.parseInt(args[1]);

    // Parses the arguments
    SparkConf conf = new SparkConf().setAppName("JavaKMeansExample");
    conf.setMaster("local");
    JavaKMeansSQLContext example = new JavaKMeansSQLContext();
    example.runKMeans(conf, inputFile, k);
  }
  
  public void runKMeans(SparkConf conf, String inputFile, int k) {
    
      JavaSparkContext jsc = new JavaSparkContext(conf);
      SQLContext sqlContext = new SQLContext(jsc);
      // Loads data
      JavaRDD<String> inputLines = jsc.textFile(inputFile);
      final Pattern separator = Pattern.compile(" ");
      
      JavaRDD<Row> rows = inputLines.map(line -> {
          String[] tok = separator.split(line);
          double[] point = new double[tok.length];
          for (int i = 0; i < tok.length; ++i) {
              point[i] = Double.parseDouble(tok[i]);
          }
          Vector[] points = {Vectors.dense(point)};
          return new GenericRow(points);
      });

      StructField[] fields = {new StructField("features", new VectorUDT(), false, Metadata.empty())};
      StructType schema = new StructType(fields);
      Dataset<Row> dataset = sqlContext.createDataFrame(rows, schema);

    // Trains a k-means model
    KMeans kmeans = new KMeans()
      .setK(k);
    KMeansModel model = kmeans.fit(dataset);
    dataset.foreach(row ->{ 
        System.out.println( "values: "+ row.get(0));
        System.out.println("prediction: "+model.predict(row.getAs(0)));
    });

    // Shows the result
    Vector[] centers = model.clusterCenters();
    System.out.println("Cluster Centers: ");
    for (Vector center: centers) {
      System.out.println(center);
    }

    jsc.stop();
  }
}
