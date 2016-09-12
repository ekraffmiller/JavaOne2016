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
public class JavaSparkSQLTest {
    JavaSparkSQL example;
    SparkSession sparkSession;
    public JavaSparkSQLTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
        example = new JavaSparkSQL();
        sparkSession = SparkSession
                .builder()
                .appName("JavaSparkSQL Unit Test")
                .master("local")
                .getOrCreate();
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of readData method, of class JavaSparkSQL.
     */
    @Test
    public void testReadData() {
        example.readData(sparkSession, "/Users/ellenk/src/spark/examples/src/main/resources/people.json");
    }
    
}
