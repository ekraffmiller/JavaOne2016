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
      
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of main method, of class KMeansCSV.
     */
    @Test
    public void testMain() {
   
        String file = "src/test/resources/metadatabibtexAbstract.csv";
        String k = "4";
        String[] args = {file, k};
        System.setProperty("spark.master", "local[2]");
        KMeansCSV.main(args);

    }
    
}
