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
import static org.junit.Assert.*;

/**
 *
 * @author ellenk
 */
public class KMeansCSVTest {
     KMeansCSV example;
    SparkSession sparkSession;
  
    public KMeansCSVTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
        example = new KMeansCSV();
           sparkSession = SparkSession
                .builder()
                .appName("Java Spark KMeans CSV Dataframe Example")
                .master("local")
                .getOrCreate();   
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of main method, of class KMeansCSV.
     */
    @Test
    public void testMain() {
    }

    /**
     * Test of runKmeans method, of class KMeansCSV.
     */
    @Test
    public void testRunKmeans() {
         String file = "metadatabibtexAbstract.csv";
              int k=5;
              example.runKmeans(sparkSession, file, k);
    }
    
}
