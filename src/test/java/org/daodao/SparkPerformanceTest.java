package org.daodao;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class SparkPerformanceTest {

    private SparkSession spark;
    private JavaSparkContext sc;
    private Random random = new Random();

    @BeforeEach
    void setUp() {
        SparkConf conf = new SparkConf()
                .setAppName("SparkPerformanceTest")
                .setMaster("local[*]")
                .set("spark.driver.host", "localhost")
                .set("spark.sql.shuffle.partitions", "4")
                .set("spark.default.parallelism", "4");

        spark = SparkSession.builder()
                .config(conf)
                .appName("SparkPerformanceTest")
                .getOrCreate();

        sc = new JavaSparkContext(spark.sparkContext());
        log.info("SparkSession initialized for performance testing");
    }

    @AfterEach
    void tearDown() {
        if (spark != null) {
            spark.close();
            log.info("SparkSession closed");
        }
    }

    @Test
    void testLargeDatasetProcessing() {
        log.info("Testing large dataset processing performance");

        // Generate large dataset
        List<Integer> largeDataset = new ArrayList<>();
        for (int i = 0; i < 100000; i++) {
            largeDataset.add(random.nextInt(1000));
        }

        long startTime = System.currentTimeMillis();

        JavaRDD<Integer> rdd = sc.parallelize(largeDataset);

        // Perform multiple transformations
        JavaRDD<Integer> processed = rdd
                .map(x -> x * 2)
                .filter(x -> x > 500)
                .map(x -> x + 100)
                .distinct();

        long count = processed.count();

        long endTime = System.currentTimeMillis();
        long processingTime = endTime - startTime;

        assertTrue(count > 0);
        assertTrue(processingTime < 10000); // Should complete within 10 seconds

        log.info("Large dataset processing completed in {}ms, processed {} records",
                processingTime, count);
    }

    @Test
    void testCachePerformance() {
        log.info("Testing cache performance");

        // Create dataset
        List<String> data = new ArrayList<>();
        for (int i = 0; i < 50000; i++) {
            data.add("item_" + i);
        }

        JavaRDD<String> rdd = sc.parallelize(data);
        JavaRDD<String> processed = rdd.map(item -> item.toUpperCase());

        // Time without cache
        long startTime = System.currentTimeMillis();
        long count1 = processed.count();
        long firstTime = System.currentTimeMillis() - startTime;

        startTime = System.currentTimeMillis();
        long count2 = processed.count();
        long secondTime = System.currentTimeMillis() - startTime;

        // Cache and test again
        JavaRDD<String> cached = processed.cache();

        startTime = System.currentTimeMillis();
        long count3 = cached.count();
        long cachedFirstTime = System.currentTimeMillis() - startTime;

        startTime = System.currentTimeMillis();
        long count4 = cached.count();
        long cachedSecondTime = System.currentTimeMillis() - startTime;

        assertEquals(count1, count2);
        assertEquals(count1, count3);
        assertEquals(count1, count4);

        // Cached second operation should be faster
        assertTrue(cachedSecondTime < cachedFirstTime);

        log.info("Cache performance test completed");
        log.info("Without cache: {}ms, {}ms | With cache: {}ms, {}ms",
                firstTime, secondTime, cachedFirstTime, cachedSecondTime);
    }

    @Test
    void testDataFramePerformance() {
        log.info("Testing DataFrame performance");

        // Create large DataFrame
        List<PerformanceData> data = new ArrayList<>();
        for (int i = 0; i < 100000; i++) {
            data.add(new PerformanceData(
                    "category_" + (i % 10),
                    random.nextInt(100),
                    random.nextDouble() * 1000,
                    System.currentTimeMillis() - random.nextInt(86400000) // Random time in last 24h
            ));
        }

        Dataset<Row> df = spark.createDataFrame(data, PerformanceData.class);

        long startTime = System.currentTimeMillis();

        // Perform complex DataFrame operations
        Dataset<Row> result = df.filter("value > 50")
                .groupBy("category")
                .avg("value", "score")
                .orderBy("category");

        List<Row> results = result.collectAsList();

        long endTime = System.currentTimeMillis();
        long processingTime = endTime - startTime;

        assertTrue(results.size() > 0);
        assertTrue(processingTime < 15000); // Should complete within 15 seconds

        log.info("DataFrame performance test completed in {}ms, returned {} categories",
                processingTime, results.size());
    }

    @Test
    void testParallelProcessing() {
        log.info("Testing parallel processing performance");

        // Test different partition sizes
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < 100000; i++) {
            data.add(i);
        }

        // Test with different numbers of partitions
        int[] partitionSizes = {2, 4, 8, 16};

        for (int partitions : partitionSizes) {
            long startTime = System.currentTimeMillis();

            JavaRDD<Integer> rdd = sc.parallelize(data, partitions);
            JavaRDD<Integer> processed = rdd
                    .map(x -> x * 2)
                    .filter(x -> x % 4 == 0)
                    .map(x -> x / 2);

            long count = processed.count();

            long endTime = System.currentTimeMillis();
            long processingTime = endTime - startTime;

            assertEquals(50000, count);

            log.info("Partitions: {}, Time: {}ms", partitions, processingTime);
        }
    }

    @Test
    void testMemoryEfficiency() {
        log.info("Testing memory efficiency");

        // Test memory usage with different operations
        List<String> largeStrings = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            largeStrings.add("This is a relatively long string with some content for testing purposes " + i);
        }

        JavaRDD<String> rdd = sc.parallelize(largeStrings);

        // Test map-side aggregation vs reduce-side aggregation
        long startTime = System.currentTimeMillis();

        // Map-side aggregation (more efficient)
        JavaRDD<Integer> lengths = rdd.map(String::length);
        long totalLength = lengths.reduce((a, b) -> a + b);

        long mapSideTime = System.currentTimeMillis() - startTime;

        startTime = System.currentTimeMillis();

        // Reduce-side aggregation (less efficient)
        long totalLength2 = rdd.map(String::length)
                .reduce((a, b) -> a + b);

        long reduceSideTime = System.currentTimeMillis() - startTime;

        assertEquals(totalLength, totalLength2);

        log.info("Memory efficiency test completed");
        log.info("Map-side: {}ms, Reduce-side: {}ms", mapSideTime, reduceSideTime);
    }

    @Test
    void testJoinPerformance() {
        log.info("Testing join performance");

        // Create two datasets for joining
        List<JoinData1> data1 = new ArrayList<>();
        List<JoinData2> data2 = new ArrayList<>();

        for (int i = 0; i < 50000; i++) {
            data1.add(new JoinData1(i, "name_" + i, "category_" + (i % 100)));
            if (i % 2 == 0) { // Only half of the keys exist in second dataset
                data2.add(new JoinData2(i, "value_" + i, random.nextDouble() * 100));
            }
        }

        Dataset<Row> df1 = spark.createDataFrame(data1, JoinData1.class);
        Dataset<Row> df2 = spark.createDataFrame(data2, JoinData2.class);

        long startTime = System.currentTimeMillis();

        // Perform join
        Dataset<Row> joined = df1.join(df2, df1.col("id").equalTo(df2.col("id")));
        long count = joined.count();

        long endTime = System.currentTimeMillis();
        long joinTime = endTime - startTime;

        assertTrue(count > 0);
        assertTrue(count < 50000); // Should be less than original due to join condition
        assertTrue(joinTime < 20000); // Should complete within 20 seconds

        log.info("Join performance test completed in {}ms, joined {} records",
                joinTime, count);
    }

    @Test
    void testOptimizationStrategies() {
        log.info("Testing optimization strategies");

        // Create test data
        List<OptimizationData> data = new ArrayList<>();
        for (int i = 0; i < 100000; i++) {
            data.add(new OptimizationData(
                    i,
                    "category_" + (i % 50),
                    random.nextInt(1000),
                    random.nextDouble() * 100
            ));
        }

        Dataset<Row> df = spark.createDataFrame(data, OptimizationData.class);

        // Strategy 1: Filter early
        long startTime = System.currentTimeMillis();

        Dataset<Row> result1 = df.filter("value > 500")
                .select("id", "category")
                .filter("category like 'category_1%'");

        long count1 = result1.count();
        long time1 = System.currentTimeMillis() - startTime;

        // Strategy 2: Select columns first (more efficient)
        startTime = System.currentTimeMillis();

        Dataset<Row> result2 = df.select("id", "category", "value")
                .filter("value > 500")
                .filter("category like 'category_1%'");

        long count2 = result2.count();
        long time2 = System.currentTimeMillis() - startTime;

        assertEquals(count1, count2);

        log.info("Optimization strategies test completed");
        log.info("Strategy 1: {}ms, Strategy 2: {}ms", time1, time2);
    }

    @Test
    void testBroadcastVariablePerformance() {
        log.info("Testing broadcast variable performance");

        // Create large lookup table
        List<String> lookupData = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            lookupData.add("key_" + i);
        }

        // Create main dataset
        List<String> mainData = new ArrayList<>();
        for (int i = 0; i < 50000; i++) {
            mainData.add("key_" + random.nextInt(10000));
        }

        // Test without broadcast
        long startTime = System.currentTimeMillis();

        JavaRDD<String> mainRDD = sc.parallelize(mainData);
        JavaRDD<String> lookupRDD = sc.parallelize(lookupData);

        JavaRDD<String> result1 = mainRDD.filter(lookupRDD.collect()::contains);
        long count1 = result1.count();

        long timeWithoutBroadcast = System.currentTimeMillis() - startTime;

        // Test with broadcast
        startTime = System.currentTimeMillis();

        scala.collection.immutable.List<String> scalaList =
                scala.collection.JavaConverters.asScalaIteratorConverter(lookupData.iterator()).asScala().toList();

        org.apache.spark.broadcast.Broadcast<scala.collection.immutable.List<String>> broadcastVar =
                sc.broadcast(scalaList);

        JavaRDD<String> result2 = mainRDD.filter(item -> broadcastVar.value().contains(item));
        long count2 = result2.count();

        long timeWithBroadcast = System.currentTimeMillis() - startTime;

        assertEquals(count1, count2);

        log.info("Broadcast variable performance test completed");
        log.info("Without broadcast: {}ms, With broadcast: {}ms", timeWithoutBroadcast, timeWithBroadcast);
    }

    // Test data classes
    public static class PerformanceData {
        private String category;
        private int value;
        private double score;
        private long timestamp;

        public PerformanceData() {}

        public PerformanceData(String category, int value, double score, long timestamp) {
            this.category = category;
            this.value = value;
            this.score = score;
            this.timestamp = timestamp;
        }

        // Getters and setters
        public String getCategory() { return category; }
        public void setCategory(String category) { this.category = category; }
        public int getValue() { return value; }
        public void setValue(int value) { this.value = value; }
        public double getScore() { return score; }
        public void setScore(double score) { this.score = score; }
        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    }

    public static class JoinData1 {
        private int id;
        private String name;
        private String category;

        public JoinData1() {}

        public JoinData1(int id, String name, String category) {
            this.id = id;
            this.name = name;
            this.category = category;
        }

        // Getters and setters
        public int getId() { return id; }
        public void setId(int id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getCategory() { return category; }
        public void setCategory(String category) { this.category = category; }
    }

    public static class JoinData2 {
        private int id;
        private String value;
        private double score;

        public JoinData2() {}

        public JoinData2(int id, String value, double score) {
            this.id = id;
            this.value = value;
            this.score = score;
        }

        // Getters and setters
        public int getId() { return id; }
        public void setId(int id) { this.id = id; }
        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }
        public double getScore() { return score; }
        public void setScore(double score) { this.score = score; }
    }

    public static class OptimizationData {
        private int id;
        private String category;
        private int value;
        private double score;

        public OptimizationData() {}

        public OptimizationData(int id, String category, int value, double score) {
            this.id = id;
            this.category = category;
            this.value = value;
            this.score = score;
        }

        // Getters and setters
        public int getId() { return id; }
        public void setId(int id) { this.id = id; }
        public String getCategory() { return category; }
        public void setCategory(String category) { this.category = category; }
        public int getValue() { return value; }
        public void setValue(int value) { this.value = value; }
        public double getScore() { return score; }
        public void setScore(double score) { this.score = score; }
    }
}