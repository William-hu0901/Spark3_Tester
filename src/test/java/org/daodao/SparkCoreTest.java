package org.daodao;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class SparkCoreTest {

    private JavaSparkContext sc;

    @BeforeEach
    void setUp() {
        SparkConf conf = new SparkConf()
                .setAppName("SparkCoreTest")
                .setMaster("local[*]")
                .set("spark.driver.host", "localhost")
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.sql.warehouse.dir", "target/spark-warehouse")
                .set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
                .set("spark.sql.adaptive.enabled", "true")
                .set("spark.sql.adaptive.coalescePartitions.enabled", "true");
        sc = new JavaSparkContext(conf);
        log.info("SparkContext initialized for testing");
    }

    @AfterEach
    void tearDown() {
        if (sc != null) {
            sc.close();
            log.info("SparkContext closed");
        }
    }

    @Test
    void testBasicRDDCreation() {
        log.info("Testing basic RDD creation");
        List<String> data = Arrays.asList("hello", "world", "spark", "testing");
        JavaRDD<String> rdd = sc.parallelize(data);

        assertEquals(4, rdd.count());
        log.info("RDD count: {}", rdd.count());
    }

    @Test
    void testMapTransformation() {
        log.info("Testing map transformation");
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = sc.parallelize(numbers);

        JavaRDD<Integer> squared = rdd.map(x -> x * x);
        List<Integer> result = squared.collect();

        assertEquals(Arrays.asList(1, 4, 9, 16, 25), result);
        log.info("Map transformation result: {}", result);
    }

    @Test
    void testFilterTransformation() {
        log.info("Testing filter transformation");
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> rdd = sc.parallelize(numbers);

        JavaRDD<Integer> evenNumbers = rdd.filter(x -> x % 2 == 0);
        List<Integer> result = evenNumbers.collect();

        assertEquals(Arrays.asList(2, 4, 6, 8, 10), result);
        log.info("Filter transformation result: {}", result);
    }

    @Test
    void testFlatMapTransformation() {
        log.info("Testing flatMap transformation");
        List<String> sentences = Arrays.asList("hello world", "spark testing", "java development");
        JavaRDD<String> rdd = sc.parallelize(sentences);

        JavaRDD<String> words = rdd.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());
        List<String> result = words.collect();

        assertEquals(6, result.size());
        assertTrue(result.contains("hello"));
        assertTrue(result.contains("spark"));
        log.info("FlatMap transformation result: {}", result);
    }

    @Test
    void testReduceByKey() {
        log.info("Testing reduceByKey transformation");
        List<Tuple2<String, Integer>> pairs = Arrays.asList(
                new Tuple2<>("spark", 1),
                new Tuple2<>("java", 1),
                new Tuple2<>("spark", 1),
                new Tuple2<>("scala", 1),
                new Tuple2<>("spark", 1)
        );

        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(pairs);
        JavaPairRDD<String, Integer> counts = pairRDD.reduceByKey((a, b) -> a + b);

        List<Tuple2<String, Integer>> result = counts.collect();
        assertEquals(3, result.size());

        Tuple2<String, Integer> sparkCount = result.stream()
                .filter(tuple -> tuple._1().equals("spark"))
                .findFirst()
                .orElse(null);

        assertNotNull(sparkCount);
        assertEquals(3, sparkCount._2().intValue());
        log.info("ReduceByKey result: {}", result);
    }

    @Test
    void testGroupByKey() {
        log.info("Testing groupByKey transformation");
        List<Tuple2<String, Integer>> pairs = Arrays.asList(
                new Tuple2<>("A", 1),
                new Tuple2<>("B", 2),
                new Tuple2<>("A", 3),
                new Tuple2<>("B", 4),
                new Tuple2<>("A", 5)
        );

        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(pairs);
        JavaPairRDD<String, Iterable<Integer>> grouped = pairRDD.groupByKey();

        List<Tuple2<String, Iterable<Integer>>> result = grouped.collect();
        assertEquals(2, result.size());
        log.info("GroupByKey result: {}", result);
    }

    @Test
    void testJoinOperation() {
        log.info("Testing join operation");
        List<Tuple2<Integer, String>> users = Arrays.asList(
                new Tuple2<>(1, "Alice"),
                new Tuple2<>(2, "Bob"),
                new Tuple2<>(3, "Charlie")
        );

        List<Tuple2<Integer, String>> departments = Arrays.asList(
                new Tuple2<>(1, "Engineering"),
                new Tuple2<>(2, "Marketing"),
                new Tuple2<>(4, "Sales")
        );

        JavaPairRDD<Integer, String> userRDD = sc.parallelizePairs(users);
        JavaPairRDD<Integer, String> deptRDD = sc.parallelizePairs(departments);

        JavaPairRDD<Integer, Tuple2<String, String>> joined = userRDD.join(deptRDD);
        List<Tuple2<Integer, Tuple2<String, String>>> result = joined.collect();

        assertEquals(2, result.size());
        log.info("Join result: {}", result);
    }

    @Test
    void testCacheAndPersist() {
        log.info("Testing cache and persist operations");
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = sc.parallelize(numbers);

        // Test cache
        JavaRDD<Integer> cached = rdd.map(x -> x * x).cache();
        long count1 = cached.count();
        long count2 = cached.count();

        assertEquals(5, count1);
        assertEquals(5, count2);
        log.info("Cache test passed with counts: {}, {}", count1, count2);

        // Test persist
        JavaRDD<Integer> persisted = rdd.map(x -> x * 2).persist(StorageLevel.MEMORY_ONLY());
        List<Integer> result = persisted.collect();
        assertEquals(Arrays.asList(2, 4, 6, 8, 10), result);
        log.info("Persist test result: {}", result);
    }

    @Test
    void testActionOperations() {
        log.info("Testing action operations");
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> rdd = sc.parallelize(numbers);

        // Test count
        long count = rdd.count();
        assertEquals(10, count);
        log.info("Count action result: {}", count);

        // Test reduce
        int sum = rdd.reduce((a, b) -> a + b);
        assertEquals(55, sum);
        log.info("Reduce action result: {}", sum);

        // Test collect
        List<Integer> collected = rdd.collect();
        assertEquals(10, collected.size());
        log.info("Collect action result size: {}", collected.size());

        // Test first
        int first = rdd.first();
        assertEquals(1, first);
        log.info("First action result: {}", first);

        // Test take
        List<Integer> firstThree = rdd.take(3);
        assertEquals(Arrays.asList(1, 2, 3), firstThree);
        log.info("Take action result: {}", firstThree);
    }

    @Test
    void testTextFileOperations() {
        log.info("Testing text file operations");
        String filePath = "src/test/resources/test-data.txt";

        JavaRDD<String> textFile = sc.textFile(filePath);
        long lineCount = textFile.count();

        assertTrue(lineCount > 0);
        log.info("Text file line count: {}", lineCount);

        // Test word count
        JavaRDD<String> words = textFile.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> wordCounts = words.mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        List<Tuple2<String, Integer>> result = wordCounts.collect();
        assertTrue(result.size() > 0);
        log.info("Word count result size: {}", result.size());
    }
}