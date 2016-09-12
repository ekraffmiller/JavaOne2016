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
public class WordCountTest {

    WordCount example;
    SparkSession sparkSession;

    public WordCountTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
        example = new WordCount();
        sparkSession = SparkSession
                .builder()
                .appName("Java Spark Simple Word Count Example")
                .master("local")
                .getOrCreate();
    }

    @After
    public void tearDown() {
    }

   
    /**
     * Test of run method, of class WordCount.
     */
    @Test
    public void testRun() {
        
  //     example.run(sparkSession,"/Users/ellenk/test/text_doc_root/Laut/docs");
        example.run(sparkSession,null);
 
    }
    
}
