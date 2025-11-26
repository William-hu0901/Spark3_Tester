package org.daodao;

import lombok.extern.slf4j.Slf4j;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * Test Suite for Spark Tester
 * This suite runs all Spark-related tests to ensure comprehensive coverage
 * of Spark functionality including Core, SQL, Streaming, MLlib, and GraphX.
 */
@Slf4j
@Suite
@SelectClasses({
    SparkCoreTest.class,
    SparkSQLTest.class,
    SparkStreamingTest.class,
    SparkMLlibTest.class,
    SparkGraphXTest.class,
    GraphDegreesAnalyzerTest.class,
    SparkIntegrationTest.class,
    SparkPerformanceTest.class
})
public class TestSuite {
    
    static {
        log.info("Initializing Spark Tester Test Suite");
        log.info("This test suite covers:");
        log.info("1. Spark Core - RDD operations, transformations, actions");
        log.info("2. Spark SQL - DataFrame operations, queries, UDFs");
        log.info("3. Spark Streaming - Real-time data processing");
        log.info("4. Spark MLlib - Machine learning algorithms");
        log.info("5. Spark GraphX - Graph processing and analysis");
        log.info("6. Spark GraphDegreesAnalyzer - Graph degree analysis");
        log.info("7. Integration Tests - Cross-component functionality");
        log.info("8. Performance Tests - Optimization and efficiency");
    }
}