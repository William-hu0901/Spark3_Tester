package org.daodao;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class GraphDegreesAnalyzerTest {

    private SparkSession spark;

    @BeforeEach
    void setUp() {
        spark = SparkSession.builder()
                .appName("GraphDegreesAnalyzerTest")
                .master("local[*]")
                .config("spark.driver.host", "localhost")
                .getOrCreate();
        
        log.info("SparkSession initialized for graph degrees analysis");
    }

    @AfterEach
    void tearDown() {
        if (spark != null) {
            spark.stop();
            log.info("SparkSession stopped");
        }
    }

    @Test
    void testGraphDegreesAnalysis() {
        log.info("Testing graph degrees analysis using Spark SQL");

        try {
            // 创建顶点数据
            List<Vertex> vertices = Arrays.asList(
                    new Vertex("a", "Alice", 34),
                    new Vertex("b", "Bob", 36),
                    new Vertex("c", "Charlie", 30),
                    new Vertex("d", "David", 29),
                    new Vertex("e", "Esther", 32)
            );

            Dataset<Row> verticesDF = spark.createDataFrame(vertices, Vertex.class);
            verticesDF.createOrReplaceTempView("vertices");

            // 创建边数据
            List<Edge> edges = Arrays.asList(
                    new Edge("a", "b", "friend"),
                    new Edge("b", "c", "follow"),
                    new Edge("c", "b", "follow"),
                    new Edge("d", "a", "friend"),
                    new Edge("e", "d", "friend"),
                    new Edge("a", "e", "friend")
            );

            Dataset<Row> edgesDF = spark.createDataFrame(edges, Edge.class);
            edgesDF.createOrReplaceTempView("edges");

            // 验证基本数据
            assertEquals(5, verticesDF.count());
            assertEquals(6, edgesDF.count());

            // 计算入度
            Dataset<Row> inDegrees = spark.sql(
                "SELECT e.dst as id, COUNT(*) as inDegree " +
                "FROM edges e " +
                "GROUP BY e.dst"
            );

            // 计算出度
            Dataset<Row> outDegrees = spark.sql(
                "SELECT e.src as id, COUNT(*) as outDegree " +
                "FROM edges e " +
                "GROUP BY e.src"
            );

            // 计算总度数
            Dataset<Row> totalDegrees = spark.sql(
                "SELECT id, SUM(degree) as totalDegree FROM (" +
                "SELECT src as id, COUNT(*) as degree FROM edges GROUP BY src " +
                "UNION ALL " +
                "SELECT dst as id, COUNT(*) as degree FROM edges GROUP BY dst" +
                ") t GROUP BY id"
            );

            // 显示结果
            log.info("=== 入度统计 ===");
            inDegrees.show();

            log.info("=== 出度统计 ===");
            outDegrees.show();

            log.info("=== 总度数统计 ===");
            totalDegrees.show();

            // 验证度数计算
            assertTrue(inDegrees.count() > 0);
            assertTrue(outDegrees.count() > 0);
            assertTrue(totalDegrees.count() > 0);

            // 验证Alice的度数 (id = 'a')
            Row aliceInDegree = inDegrees.filter("id = 'a'").first();
            Row aliceOutDegree = outDegrees.filter("id = 'a'").first();
            Row aliceTotalDegree = totalDegrees.filter("id = 'a'").first();

            assertEquals(1, aliceInDegree.getLong(1)); // Alice has 1 incoming edge (from d)
            assertEquals(2, aliceOutDegree.getLong(1)); // Alice has 2 outgoing edges (to b, e)
            assertEquals(3, aliceTotalDegree.getLong(1)); // Total degree = 1 + 2

            log.info("Graph degrees analysis test completed successfully");

        } catch (Exception e) {
            log.error("Error during graph degrees analysis", e);
            fail("Graph degrees analysis failed: " + e.getMessage());
        }
    }

    @Test
    void testGraphDegreesStatistics() {
        log.info("Testing graph degrees statistics calculation");

        try {
            // 创建一个更复杂的图用于统计测试
            List<Vertex> vertices = Arrays.asList(
                    new Vertex("1", "Node1", 25),
                    new Vertex("2", "Node2", 30),
                    new Vertex("3", "Node3", 35),
                    new Vertex("4", "Node4", 40),
                    new Vertex("5", "Node5", 45)
            );

            Dataset<Row> verticesDF = spark.createDataFrame(vertices, Vertex.class);
            verticesDF.createOrReplaceTempView("vertices_stats");

            List<Edge> edges = Arrays.asList(
                    new Edge("1", "2", "connect"),
                    new Edge("1", "3", "connect"),
                    new Edge("2", "3", "connect"),
                    new Edge("3", "4", "connect"),
                    new Edge("4", "5", "connect"),
                    new Edge("5", "1", "connect"),
                    new Edge("2", "4", "connect")
            );

            Dataset<Row> edgesDF = spark.createDataFrame(edges, Edge.class);
            edgesDF.createOrReplaceTempView("edges_stats");

            // 计算度数
            Dataset<Row> degrees = spark.sql(
                "SELECT id, SUM(degree) as totalDegree FROM (" +
                "SELECT src as id, COUNT(*) as degree FROM edges_stats GROUP BY src " +
                "UNION ALL " +
                "SELECT dst as id, COUNT(*) as degree FROM edges_stats GROUP BY dst" +
                ") t GROUP BY id"
            );

            // 计算统计信息
            Dataset<Row> stats = degrees.agg(
                    max("totalDegree").as("最大度数"),
                    min("totalDegree").as("最小度数"),
                    avg("totalDegree").as("平均度数"),
                    sum("totalDegree").as("总度数")
            );

            Row statsRow = stats.first();
            long maxDegree = statsRow.getLong(0);
            long minDegree = statsRow.getLong(1);
            double avgDegree = statsRow.getDouble(2);
            long totalDegree = statsRow.getLong(3);

            // 验证统计结果
            assertTrue(maxDegree >= minDegree);
            assertTrue(avgDegree > 0);
            assertTrue(totalDegree > 0);

            log.info("度数统计 - 最大: {}, 最小: {}, 平均: {}, 总计: {}", 
                    maxDegree, minDegree, avgDegree, totalDegree);

            // 测试度数分布
            Dataset<Row> degreeDistribution = degrees.groupBy("totalDegree")
                    .count()
                    .orderBy("totalDegree");

            long distributionCount = degreeDistribution.count();
            assertTrue(distributionCount > 0);

            log.info("度数分布:");
            degreeDistribution.show();

        } catch (Exception e) {
            log.error("Error during statistics calculation", e);
            fail("Statistics calculation failed: " + e.getMessage());
        }
    }

    @Test
    void testGraphDegreesWithEmptyGraph() {
        log.info("Testing graph degrees with empty graph");

        try {
            // 创建空图
            List<Vertex> emptyVertices = Arrays.asList();
            List<Edge> emptyEdges = Arrays.asList();

            Dataset<Row> verticesDF = spark.createDataFrame(emptyVertices, Vertex.class);
            Dataset<Row> edgesDF = spark.createDataFrame(emptyEdges, Edge.class);

            verticesDF.createOrReplaceTempView("empty_vertices");
            edgesDF.createOrReplaceTempView("empty_edges");

            // 计算度数
            Dataset<Row> inDegrees = spark.sql(
                "SELECT e.dst as id, COUNT(*) as inDegree " +
                "FROM empty_edges e " +
                "GROUP BY e.dst"
            );

            Dataset<Row> outDegrees = spark.sql(
                "SELECT e.src as id, COUNT(*) as outDegree " +
                "FROM empty_edges e " +
                "GROUP BY e.src"
            );

            Dataset<Row> totalDegrees = spark.sql(
                "SELECT id, SUM(degree) as totalDegree FROM (" +
                "SELECT src as id, COUNT(*) as degree FROM empty_edges GROUP BY src " +
                "UNION ALL " +
                "SELECT dst as id, COUNT(*) as degree FROM empty_edges GROUP BY dst" +
                ") t GROUP BY id"
            );

            // 验证空图的度数
            assertEquals(0, inDegrees.count());
            assertEquals(0, outDegrees.count());
            assertEquals(0, totalDegrees.count());

            log.info("Empty graph degrees test passed");

        } catch (Exception e) {
            log.error("Error with empty graph", e);
            fail("Empty graph test failed: " + e.getMessage());
        }
    }

    @Test
    void testGraphDegreesWithSingleNode() {
        log.info("Testing graph degrees with single node");

        try {
            // 创建单节点图
            List<Vertex> vertices = Arrays.asList(
                    new Vertex("single", "SingleNode", 50)
            );

            List<Edge> edges = Arrays.asList();

            Dataset<Row> verticesDF = spark.createDataFrame(vertices, Vertex.class);
            Dataset<Row> edgesDF = spark.createDataFrame(edges, Edge.class);

            verticesDF.createOrReplaceTempView("single_vertices");
            edgesDF.createOrReplaceTempView("single_edges");

            // 计算度数
            Dataset<Row> inDegrees = spark.sql(
                "SELECT e.dst as id, COUNT(*) as inDegree " +
                "FROM single_edges e " +
                "GROUP BY e.dst"
            );

            Dataset<Row> outDegrees = spark.sql(
                "SELECT e.src as id, COUNT(*) as outDegree " +
                "FROM single_edges e " +
                "GROUP BY e.src"
            );

            Dataset<Row> totalDegrees = spark.sql(
                "SELECT id, SUM(degree) as totalDegree FROM (" +
                "SELECT src as id, COUNT(*) as degree FROM single_edges GROUP BY src " +
                "UNION ALL " +
                "SELECT dst as id, COUNT(*) as degree FROM single_edges GROUP BY dst" +
                ") t GROUP BY id"
            );

            // 验证单节点图的度数
            assertEquals(0, inDegrees.count());
            assertEquals(0, outDegrees.count());
            assertEquals(0, totalDegrees.count()); // No edges = no degrees

            log.info("Single node graph degrees test passed");

        } catch (Exception e) {
            log.error("Error with single node graph", e);
            fail("Single node graph test failed: " + e.getMessage());
        }
    }

    @Test
    void testGraphDegreesWithDataFrameAPI() {
        log.info("Testing graph degrees using DataFrame API");

        try {
            // 创建顶点和边数据
            List<Vertex> vertices = Arrays.asList(
                    new Vertex("a", "Alice", 34),
                    new Vertex("b", "Bob", 36),
                    new Vertex("c", "Charlie", 30)
            );

            List<Edge> edges = Arrays.asList(
                    new Edge("a", "b", "friend"),
                    new Edge("b", "c", "follow"),
                    new Edge("c", "a", "follow")
            );

            Dataset<Row> verticesDF = spark.createDataFrame(vertices, Vertex.class);
            Dataset<Row> edgesDF = spark.createDataFrame(edges, Edge.class);

            // 使用DataFrame API计算入度
            Dataset<Row> inDegrees = edgesDF.groupBy("dst")
                    .count()
                    .withColumnRenamed("dst", "id")
                    .withColumnRenamed("count", "inDegree");

            // 使用DataFrame API计算出度
            Dataset<Row> outDegrees = edgesDF.groupBy("src")
                    .count()
                    .withColumnRenamed("src", "id")
                    .withColumnRenamed("count", "outDegree");

            // 计算总度数
            Dataset<Row> allDegrees = inDegrees.unionByName(outDegrees, true)
                    .groupBy("id")
                    .agg(
                            sum("inDegree").as("totalInDegree"),
                            sum("outDegree").as("totalOutDegree")
                    )
                    .withColumn("totalDegree", col("totalInDegree").plus(col("totalOutDegree")));

            // 显示结果
            log.info("=== DataFrame API 入度统计 ===");
            inDegrees.show();

            log.info("=== DataFrame API 出度统计 ===");
            outDegrees.show();

            log.info("=== DataFrame API 总度数统计 ===");
            allDegrees.show();

            // 验证结果
            assertTrue(inDegrees.count() > 0);
            assertTrue(outDegrees.count() > 0);
            assertTrue(allDegrees.count() > 0);

            log.info("DataFrame API graph degrees test passed");

        } catch (Exception e) {
            log.error("Error with DataFrame API", e);
            fail("DataFrame API test failed: " + e.getMessage());
        }
    }

    // 顶点数据类
    public static class Vertex implements Serializable {
        private String id;
        private String name;
        private int age;

        public Vertex() {}

        public Vertex(String id, String name, int age) {
            this.id = id;
            this.name = name;
            this.age = age;
        }

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public int getAge() { return age; }
        public void setAge(int age) { this.age = age; }
    }

    // 边数据类
    public static class Edge implements Serializable {
        private String src;
        private String dst;
        private String relationship;

        public Edge() {}

        public Edge(String src, String dst, String relationship) {
            this.src = src;
            this.dst = dst;
            this.relationship = relationship;
        }

        public String getSrc() { return src; }
        public void setSrc(String src) { this.src = src; }

        public String getDst() { return dst; }
        public void setDst(String dst) { this.dst = dst; }

        public String getRelationship() { return relationship; }
        public void setRelationship(String relationship) { this.relationship = relationship; }
    }
}