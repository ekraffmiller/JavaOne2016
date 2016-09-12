/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.harvard.iq.javaone2016;

import org.apache.spark.sql.SparkSession;

/**
 *
 * @author ellenk
 */
public class Tfidf {
    public static void main(String args) {
           
        SparkSession spark = SparkSession
                .builder()
                .appName("Java KMeans Session Example")
           //     .master("local[2]") comment out to specify master on spark-submit command line
                .getOrCreate();
    }
    public void extractFeatures(SparkSession spark) {
        
    }
}
