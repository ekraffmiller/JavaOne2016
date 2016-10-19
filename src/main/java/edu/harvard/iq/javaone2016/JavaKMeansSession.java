
// spark-submit:
// ./spark-submit --class edu.harvard.iq.javaone2016.JavaKMeansSession /Users/ellenk/src/JavaOne2016/target/JavaOne2016-1.0-SNAPSHOT.jar "/Applications/spark-2.0.0-bin-hadoop2.7/data/mllib/kmeans_data.txt" 2
package edu.harvard.iq.javaone2016;

import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;



// /Applications/spark-2.0.0-bin-hadoop2.7/bin/spark-submit --class edu.harvard.iq.javaone2016.JavaKMeansSession --master spark://Ellens-MacBook-Pro.local:7077 --verbose /Users/ellenk/src/JavaOne2016/target/JavaOne2016-1.0-SNAPSHOT.jar "/Applications/spark-2.0.0-bin-hadoop2.7/data/mllib/kmeans_data.txt" 2  


public class JavaKMeansSession {

   

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: JavaKMeansSession <file> <k>");
            System.exit(1);
        }
        String master = "local";
        String runtimeMaster = System.getProperty("spark.master");
        System.out.println("runtimeMaster: "+runtimeMaster);
        if (runtimeMaster!=null) {
            master = runtimeMaster;
        }
        String inputFile = args[0];
        int k = Integer.parseInt(args[1]);
        // Parses the arguments
       
        JavaKMeansSession example = new JavaKMeansSession();
       
        SparkSession spark = SparkSession
                .builder()
                .appName("Java KMeans Session Example")
                .master(master) 
                .getOrCreate();

        example.runKmeans(spark, inputFile, k);
    }

    public void runKmeans(SparkSession spark, String inputFile, int k) {
        final Pattern separator = Pattern.compile(" ");

        JavaRDD<String> linesRDD = spark.read().textFile(inputFile).toJavaRDD();

        JavaRDD<Row> pointsRDD = linesRDD.map(line -> {
            String[] tok = separator.split(line);
            double[] point = new double[tok.length];
            for (int i = 0; i < tok.length; ++i) {
                point[i] = Double.parseDouble(tok[i]);
            }
            Vector[] points = {Vectors.dense(point)};
            return new GenericRow(points);
        });

        // Loads data
        StructField[] fields = {new StructField("features", new VectorUDT(), false, Metadata.empty())};
        StructType schema = new StructType(fields);
        Dataset<Row> dataset = spark.createDataFrame(pointsRDD, schema);

        // Trains a k-means model
        KMeans kmeans = new KMeans()
                .setK(k);
        KMeansModel model = kmeans.fit(dataset);

        // Shows the result
        Vector[] centers = model.clusterCenters();
        System.out.println("Cluster Centers: ");
        for (Vector center : centers) {
            System.out.println(center);
        }
        dataset.foreach(row -> {
            System.out.println("values: " + row.get(0));
            System.out.println("prediction: " + model.predict(row.getAs(0)));
        });
        spark.stop();
    }

}
