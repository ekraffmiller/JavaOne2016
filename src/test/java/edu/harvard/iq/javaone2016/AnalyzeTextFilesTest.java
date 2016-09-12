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
import org.junit.Test;

/**
 *
 * @author ellenk
 */
public class AnalyzeTextFilesTest {
    AnalyzeTextFiles example;
    SparkSession sparkSession;
    public AnalyzeTextFilesTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
   example = new AnalyzeTextFiles();
           sparkSession = SparkSession
                .builder()
                .appName("Java Spark KMeans Dataframe Example")
                .master("local")
                .getOrCreate();   }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of main method, of class AnalyzeTextFiles.
     */
    @Test
    public void testMain() {
    }

    /**
     * Test of runKmeans method, of class AnalyzeTextFiles.
     */
    @Test
    public void testRunKmeans() {
              String dir = "/Users/ellenk/test/text_doc_root/Laut/docs";
              int k=4;
              example.runKmeans(sparkSession, dir, k);
 
    }
    
}
