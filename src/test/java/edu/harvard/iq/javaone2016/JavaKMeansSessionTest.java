/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.harvard.iq.javaone2016;

import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 *
 * @author ellenk
 */
public class JavaKMeansSessionTest {
    JavaKMeansSession example;
    SparkSession spark;
    public JavaKMeansSessionTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
           example = new JavaKMeansSession();
           spark = SparkSession
                .builder()
                .appName("Java Spark KMeans Dataframe Example")
                .master("local")
                .getOrCreate();
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of main method, of class JavaKMeansSession.
     */
    @org.junit.Test
    public void testMain() {
    }

    /**
     * Test of runKmeans method, of class JavaKMeansSession.
     */
    @org.junit.Test
    public void testRunKmeans() {
         String inputFile = "kmeans_data.txt";
        int k = 2;
        example.runKmeans(spark, inputFile, k);
    }
    
}
