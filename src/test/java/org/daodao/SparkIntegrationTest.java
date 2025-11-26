package org.daodao;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class SparkIntegrationTest {

    private SparkSession spark;

    @BeforeEach
    void setUp() {
        SparkConf conf = new SparkConf()
                .setAppName("SparkIntegrationTest")
                .setMaster("local[1]") // Use single thread to avoid issues
                .set("spark.driver.host", "localhost")
                .set("spark.driver.bindAddress", "127.0.0.1")
                .set("spark.network.timeout", "300s")
                .set("spark.executor.heartbeatInterval", "60s")
                .set("spark.rpc.askTimeout", "300s")
                .set("spark.rpc.lookupTimeout", "300s")
                .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
                .set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
                .set("spark.hadoop.fs.local.impl", "org.apache.hadoop.fs.LocalFileSystem")
                .set("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir"))
                .set("spark.sql.adaptive.enabled", "false")
                .set("spark.sql.adaptive.coalescePartitions.enabled", "false");

        spark = SparkSession.builder()
                .config(conf)
                .appName("SparkIntegrationTest")
                .getOrCreate();

        log.info("SparkSession initialized for integration testing");
    }

    @AfterEach
    void tearDown() {
        if (spark != null) {
            spark.close();
            log.info("SparkSession closed");
        }
    }

    @Test
    void testBasicRDDOperations() {
        log.info("Testing basic RDD operations");

        // Create simple RDD
        List<String> data = Arrays.asList("hello", "world", "spark", "test");
        JavaRDD<String> rdd = new JavaSparkContext(spark.sparkContext()).parallelize(data);

        // Test basic operations
        assertEquals(4, rdd.count());
        assertEquals(19, rdd.map(String::length).reduce(Integer::sum).intValue());

        // Test filter
        JavaRDD<String> filtered = rdd.filter(s -> s.length() > 4);
        assertEquals(3, filtered.count());
        assertTrue(filtered.collect().contains("hello"));

        log.info("Basic RDD operations test completed");
    }

    @Test
    void testBasicDataFrameOperations() {
        log.info("Testing basic DataFrame operations");

        // Create simple DataFrame
        List<Person> people = Arrays.asList(
                new Person("Alice", 25, "New York"),
                new Person("Bob", 30, "San Francisco")
        );

        Dataset<Row> df = spark.createDataFrame(people, Person.class);

        // Test basic operations
        assertEquals(2, df.count());
        assertEquals("Alice", df.select("name").first().getString(0));

        // Test filter
        Dataset<Row> filtered = df.filter("age > 28");
        assertEquals(1, filtered.count());
        assertEquals("Bob", filtered.select("name").first().getString(0));

        log.info("Basic DataFrame operations test completed");
    }

    @Test
    void testRDDToDataFrameConversion() {
        log.info("Testing RDD to DataFrame conversion");

        // Create RDD from list of Person objects
        List<Person> people = Arrays.asList(
                new Person("Alice", 25, "New York"),
                new Person("Bob", 30, "San Francisco"),
                new Person("Charlie", 35, "Chicago")
        );

        JavaRDD<Person> personRDD = new JavaSparkContext(spark.sparkContext()).parallelize(people);

        // Convert RDD to DataFrame
        Dataset<Row> df = spark.createDataFrame(personRDD, Person.class);

        // Verify conversion
        assertEquals(3, df.count());
        assertEquals("Alice", df.select("name").first().getString(0));

        // Test SQL operations on converted DataFrame
        df.createOrReplaceTempView("people");
        Dataset<Row> result = spark.sql("SELECT * FROM people WHERE age > 28");
        assertEquals(2, result.count());

        log.info("RDD to DataFrame conversion test completed");
    }

    @Test
    void testDataFrameToRDDConversion() {
        log.info("Testing DataFrame to RDD conversion");

        // Create DataFrame
        List<Person> people = Arrays.asList(
                new Person("Alice", 25, "New York"),
                new Person("Bob", 30, "San Francisco")
        );

        Dataset<Row> df = spark.createDataFrame(people, Person.class);

        // Convert DataFrame to RDD
        JavaRDD<Row> rowRDD = df.javaRDD();

        // Verify conversion
        assertEquals(2, rowRDD.count());

        Row firstRow = rowRDD.first();
        assertEquals("Alice", firstRow.getString(firstRow.fieldIndex("name")));
        assertEquals(25, firstRow.getInt(firstRow.fieldIndex("age")));
        assertEquals("New York", firstRow.getString(firstRow.fieldIndex("city")));

        log.info("DataFrame to RDD conversion test completed");
    }

    @Test
    void testSimpleDataProcessingPipeline() {
        log.info("Testing simple data processing pipeline");

        // Create initial data
        List<String> rawData = Arrays.asList(
                "Alice,25,New York,50000",
                "Bob,30,San Francisco,60000",
                "Charlie,35,Chicago,70000"
        );

        // Step 1: Create RDD and parse data
        JavaRDD<String> rawRDD = new JavaSparkContext(spark.sparkContext()).parallelize(rawData);
        JavaRDD<Employee> employeeRDD = rawRDD.map(line -> {
            String[] parts = line.split(",");
            return new Employee(parts[0], Integer.parseInt(parts[1]),
                    parts[2], Double.parseDouble(parts[3]));
        });

        // Step 2: Convert to DataFrame
        Dataset<Row> employeeDF = spark.createDataFrame(employeeRDD, Employee.class);
        employeeDF.createOrReplaceTempView("employees");

        // Step 3: Perform SQL transformations
        Dataset<Row> highEarners = spark.sql(
                "SELECT name, age, city, salary FROM employees WHERE salary > 55000");

        // Verify pipeline results
        assertEquals(2, highEarners.count());

        List<Row> results = highEarners.collectAsList();
        assertTrue(results.stream().anyMatch(row -> row.getString(0).equals("Charlie")));

        log.info("Simple data processing pipeline test completed");
        log.info("Pipeline processed {} records", highEarners.count());
    }

    @Test
    void testErrorHandling() {
        log.info("Testing error handling");

        // Test with malformed data
        List<String> malformedData = Arrays.asList(
                "Alice,25,New York",
                "Bob,invalid_age,San Francisco",
                "Charlie,35,Chicago"
        );

        JavaRDD<String> rawRDD = new JavaSparkContext(spark.sparkContext()).parallelize(malformedData);

        // Process with error handling
        JavaRDD<Person> processedRDD = rawRDD.map(line -> {
            try {
                String[] parts = line.split(",");
                return new Person(parts[0], Integer.parseInt(parts[1]), parts[2]);
            } catch (Exception e) {
                log.warn("Error processing line: {}", line, e);
                return null; // Return null for malformed records
            }
        }).filter(person -> person != null); // Filter out null records

        // Verify error handling
        long validRecords = processedRDD.count();
        assertEquals(2, validRecords); // Only 2 out of 3 records should be valid

        log.info("Error handling test completed");
        log.info("Valid records processed: {}", validRecords);
    }

    // Test data classes
    public static class Person implements java.io.Serializable {
        private String name;
        private int age;
        private String city;

        public Person() {}

        public Person(String name, int age, String city) {
            this.name = name;
            this.age = age;
            this.city = city;
        }

        // Getters and setters
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public int getAge() { return age; }
        public void setAge(int age) { this.age = age; }
        public String getCity() { return city; }
        public void setCity(String city) { this.city = city; }
    }

    public static class Employee implements java.io.Serializable {
        private String name;
        private int age;
        private String city;
        private double salary;

        public Employee() {}

        public Employee(String name, int age, String city, double salary) {
            this.name = name;
            this.age = age;
            this.city = city;
            this.salary = salary;
        }

        // Getters and setters
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public int getAge() { return age; }
        public void setAge(int age) { this.age = age; }
        public String getCity() { return city; }
        public void setCity(String city) { this.city = city; }
        public double getSalary() { return salary; }
        public void setSalary(double salary) { this.salary = salary; }
    }
}