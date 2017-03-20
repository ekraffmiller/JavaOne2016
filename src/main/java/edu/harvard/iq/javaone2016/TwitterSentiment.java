
package edu.harvard.iq.javaone2016;

import java.util.List;

import com.google.common.collect.Lists;
import java.io.IOException;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class TwitterSentiment {

    public static void main(String[] args) {
        String csvFile = args[0];
        SparkSession spark = SparkSession
                .builder()
                .appName("Twitter Example")
                .getOrCreate();

        // Prepare training documents, which are labeled.
      /*  List<LabeledDocument> localTraining = Lists.newArrayList(
                new LabeledDocument(0L, "a b c d e spark", 1.0),
                new LabeledDocument(1L, "b d", 0.0),
                new LabeledDocument(2L, "spark f g h", 1.0),
                new LabeledDocument(3L, "hadoop mapreduce", 0.0));
        
        Dataset<LabeledDocument> trainingData = spark.createDataset(localTraining, Encoders.bean(LabeledDocument.class));
       */ Dataset<Row> fileData = spark.read().option("header", true).option("inferSchema",true ).csv(csvFile);
      
        
        // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
        Tokenizer tokenizer = new Tokenizer()
                .setInputCol("text")
                .setOutputCol("words");
        HashingTF hashingTF = new HashingTF()
                .setNumFeatures(1000)
                .setInputCol(tokenizer.getOutputCol())
                .setOutputCol("features");
        LogisticRegression lr = new LogisticRegression()
                .setMaxIter(10)
                .setRegParam(0.001);
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{tokenizer, hashingTF, lr});
    
        // Fit the pipeline to training documents.
        PipelineModel model = pipeline.fit(fileData);
        
        // Save the pipeline and model for future use
         try {
            pipeline.write().overwrite().save("/tmp/spark-regression-workflow");
            model.write().overwrite().save("/tmp/spark-regression-model");
        } catch (IOException e) {
            System.out.println("error saving model file");
        }

        // Prepare test documents, which are unlabeled.
     /*            628,  "such a nice day"
629,1,Sentiment140,  Summertime by DJ Jazzy Jeff and The Fresh Prince
630,0,Sentiment140,  The iPhone will be out of commission for a while.
631,1,Sentiment140,  the weather is lushhh!
632,0,Sentiment140,  Things are not good in my head. I may not be around a lot this weekend.
*/
       List<Document> localTest = Lists.newArrayList(
                new Document(4L, "such a nice day"),
                new Document(5L, "Devoxx US is the best!"),
                new Document(6L, "The iPhone will be out of commission for a while."),
                new Document(7L, "Things are not good in my head. I may not be around a lot this weekend."));
        
        Dataset<Document> test = spark.createDataset(localTest, Encoders.bean(Document.class));

        // Make predictions on test documents.
        Dataset<Row> predictions = model.transform(test);
        List<Row> result = predictions.select("id", "text", "probability", "prediction").collectAsList();
        for (Row r : result) {
            System.out.println("(" + r.get(0) + ", " + r.get(1) + ") --> prob=" + r.get(2)
                    + ", prediction=" + r.get(3));
        }
        spark.stop();

    }

    public static class LabeledDocument {

        long id;
        String text;
        Double label;

        public LabeledDocument(long id, String text, Double label) {
            this.id = id;
            this.text = text;
            this.label = label;
        }

        public LabeledDocument() {
        }

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public String getText() {
            return text;
        }

        public void setText(String text) {
            this.text = text;
        }

        public Double getLabel() {
            return label;
        }

        public void setLabel(Double label) {
            this.label = label;
        }

    }

    public static class Document {

        long id;
        String text;

        public Document() {
        }

        Document(long id, String text) {
            this.id = id;
            this.text = text;
        }

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public String getText() {
            return text;
        }

        public void setText(String text) {
            this.text = text;
        }

    }
}
