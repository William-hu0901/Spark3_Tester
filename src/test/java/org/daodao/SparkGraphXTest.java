package org.daodao;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class SparkGraphXTest {

    private JavaSparkContext sc;
    private ClassTag<String> stringTag = ClassTag$.MODULE$.apply(String.class);
    private ClassTag<Integer> intTag = ClassTag$.MODULE$.apply(Integer.class);

    @BeforeEach
    void setUp() {
        SparkConf conf = new SparkConf()
                .setAppName("SparkGraphXTest")
                .setMaster("local[1]")
                .set("spark.driver.host", "localhost")
                .set("spark.driver.bindAddress", "127.0.0.1")
                .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
                .set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
                .set("spark.hadoop.fs.local.impl", "org.apache.hadoop.fs.LocalFileSystem");
        sc = new JavaSparkContext(conf);
        log.info("SparkContext initialized for GraphX testing");
    }

    @AfterEach
    void tearDown() {
        if (sc != null) {
            sc.close();
            log.info("SparkContext closed");
        }
    }

    @Test
    void testCreateSimpleGraph() {
        log.info("Testing simple graph creation");

        // Create vertices
        List<Tuple2<Object, String>> vertices = Arrays.asList(
                new Tuple2<>(1L, "Alice"),
                new Tuple2<>(2L, "Bob"),
                new Tuple2<>(3L, "Charlie")
        );

        // Create edges using Edge.apply
        List<Edge<Integer>> edges = Arrays.asList(
                Edge.apply(1L, 2L, 1),
                Edge.apply(2L, 3L, 1)
        );

        // Convert to RDDs
        RDD<Tuple2<Object, String>> verticesRDD = sc.parallelize(vertices).rdd();
        RDD<Edge<Integer>> edgesRDD = sc.parallelize(edges).rdd();

        // Create graph
        Graph<String, Integer> graph = Graph.apply(verticesRDD, edgesRDD, "default", 
                StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), stringTag, intTag);

        // Verify graph properties
        assertEquals(3, graph.vertices().count());
        assertEquals(2, graph.edges().count());

        log.info("Simple graph created with {} vertices and {} edges",
                graph.vertices().count(), graph.edges().count());
    }

    @Test
    void testBasicGraphOperations() {
        log.info("Testing basic graph operations");

        // Create vertices
        List<Tuple2<Object, String>> vertices = Arrays.asList(
                new Tuple2<>(1L, "A"),
                new Tuple2<>(2L, "B"),
                new Tuple2<>(3L, "C")
        );

        // Create edges
        List<Edge<Integer>> edges = Arrays.asList(
                Edge.apply(1L, 2L, 5),
                Edge.apply(2L, 3L, 3)
        );

        RDD<Tuple2<Object, String>> verticesRDD = sc.parallelize(vertices).rdd();
        RDD<Edge<Integer>> edgesRDD = sc.parallelize(edges).rdd();

        Graph<String, Integer> graph = Graph.apply(verticesRDD, edgesRDD, "default",
                StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), stringTag, intTag);

        // Test basic operations
        assertEquals(3, graph.vertices().count());
        assertEquals(2, graph.edges().count());

        log.info("Basic graph operations test completed");
    }
}