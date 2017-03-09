/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.harvard.iq.javaone2016;

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
public class WordCountRDDTest {
    
    public WordCountRDDTest() {
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
     * Test of main method, of class WordCountRDD.
     */
    @Test
    public void testMain() {
       
        System.setProperty("spark.master","local[2]");   
        WordCountRDD.main(new String[]{});
    }

    /**
     * Test of run method, of class WordCountRDD.
     */
    @Test
    public void testRun() {
    }
    
}
