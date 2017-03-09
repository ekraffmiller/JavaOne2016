
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


public class ClassificationPipeline {

    public static void main(String[] args) {
    
        SparkSession spark = SparkSession
                .builder()
                .appName("Classification Example")
                .getOrCreate();

        // Prepare training documents, which are labeled.
        List<LabeledDocument> localTraining = Lists.newArrayList(
                new LabeledDocument(0L, "a b c d e spark", 1.0),
                new LabeledDocument(1L, "b d", 0.0),
                new LabeledDocument(2L, "spark f g h", 1.0),
                new LabeledDocument(3L, "hadoop mapreduce", 0.0));
        
        Dataset<LabeledDocument> trainingData = spark.createDataset(localTraining, Encoders.bean(LabeledDocument.class));
       
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
        PipelineModel model = pipeline.fit(trainingData);
        
        // Save the pipeline and model for future use
         try {
            pipeline.write().overwrite().save("/tmp/spark-regression-workflow");
            model.write().overwrite().save("/tmp/spark-regression-model");
        } catch (IOException e) {
            System.out.println("error saving model file");
        }

        // Prepare test documents, which are unlabeled.
        List<Document> localTest = Lists.newArrayList(
                new Document(4L, "spark i j k"),
                new Document(5L, "l m n"),
                new Document(6L, "spark hadoop spark"),
                new Document(7L, "apache hadoop"));
        
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
