/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.harvard.iq.javaone2016;

import org.apache.spark.SparkConf;
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
public class JavaKMeansSQLContextTest {
    SparkConf conf;
    JavaKMeansSQLContext example;
    public JavaKMeansSQLContextTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() { 
        // Setup Spark Config
        conf = new SparkConf().setAppName("JavaKMeansExample");
        conf.setMaster("local");
        example = new JavaKMeansSQLContext();    
    }
    
    @After
    public void tearDown() {
    }

  

    /**
     * Test of runKMeans method, of class JavaKMeansSQLContext.
     */
    @Test
    public void testRunKMeans() {
        String inputFile = "/Applications/spark-2.0.0-bin-hadoop2.7/data/mllib/kmeans_data.txt";
        int k = 2;
        example.runKMeans(conf, inputFile, k);
    }
    
}
