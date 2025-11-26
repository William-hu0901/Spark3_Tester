package org.daodao;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.spark.sql.functions.*;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class SparkSQLTest {

    private SparkSession spark;
    private JavaSparkContext sc;

    @BeforeEach
    void setUp() {
        SparkConf conf = new SparkConf()
                .setAppName("SparkSQLTest")
                .setMaster("local[*]")
                .set("spark.driver.host", "localhost")
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.sql.warehouse.dir", "target/spark-warehouse")
                .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
                .set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
                .set("spark.sql.adaptive.enabled", "true")
                .set("spark.sql.adaptive.coalescePartitions.enabled", "true");

        spark = SparkSession.builder()
                .config(conf)
                .appName("SparkSQLTest")
                .getOrCreate();

        sc = new JavaSparkContext(spark.sparkContext());
        log.info("SparkSession initialized for testing");
    }

    @AfterEach
    void tearDown() {
        if (spark != null) {
            spark.close();
            log.info("SparkSession closed");
        }
    }

    @Test
    void testCreateDataFrameFromList() {
        log.info("Testing DataFrame creation from list");

        // Create a list of people
        List<Person> people = Arrays.asList(
                new Person("Alice", 25, "New York"),
                new Person("Bob", 30, "San Francisco"),
                new Person("Charlie", 35, "Chicago")
        );

        Dataset<Row> df = spark.createDataFrame(people, Person.class);

        assertEquals(3, df.count());
        assertEquals("Alice", df.select("name").first().getString(0));
        log.info("DataFrame created successfully with {} rows", df.count());
    }

    @Test
    void testBasicSQLQueries() {
        log.info("Testing basic SQL queries");

        List<Person> people = Arrays.asList(
                new Person("Alice", 25, "New York"),
                new Person("Bob", 30, "San Francisco"),
                new Person("Charlie", 35, "Chicago"),
                new Person("Diana", 28, "Boston")
        );

        Dataset<Row> df = spark.createDataFrame(people, Person.class);
        df.createOrReplaceTempView("people");

        // Test SELECT query
        Dataset<Row> result = spark.sql("SELECT name, age FROM people WHERE age > 28");
        assertEquals(2, result.count());
        log.info("SQL query returned {} rows", result.count());

        // Test aggregation
        Dataset<Row> avgAge = spark.sql("SELECT AVG(age) as avg_age FROM people");
        Row avgRow = avgAge.first();
        assertTrue(avgRow.getDouble(0) > 25);
        log.info("Average age: {}", avgRow.getDouble(0));
    }

    @Test
    void testDataFrameOperations() {
        log.info("Testing DataFrame operations");

        List<Person> people = Arrays.asList(
                new Person("Alice", 25, "New York"),
                new Person("Bob", 30, "San Francisco"),
                new Person("Charlie", 35, "Chicago"),
                new Person("Diana", 28, "Boston"),
                new Person("Eve", 32, "Seattle")
        );

        Dataset<Row> df = spark.createDataFrame(people, Person.class);

        // Test filter
        Dataset<Row> filtered = df.filter("age > 30");
        assertEquals(2, filtered.count());
        log.info("Filtered DataFrame count: {}", filtered.count());

        // Test select
        Dataset<Row> selected = df.select("name", "city");
        assertEquals(2, selected.columns().length);
        log.info("Selected columns: {}", Arrays.toString(selected.columns()));

        // Test groupBy
        Dataset<Row> grouped = df.groupBy("city").count();
        long cityCount = grouped.count();
        assertEquals(5, cityCount);
        log.info("Grouped by city count: {}", cityCount);

        // Test orderBy
        Dataset<Row> ordered = df.orderBy("age");
        Row first = ordered.first();
        assertEquals("Alice", first.getString(first.fieldIndex("name")));
        log.info("Ordered by age, first person: {}", first.getString(first.fieldIndex("name")));
    }

    @Test
    void testJSONFileOperations() {
        log.info("Testing JSON file operations");

        String jsonPath = "src/test/resources/people.json";
        Dataset<Row> jsonDF = spark.read().json(jsonPath);

        assertTrue(jsonDF.count() > 0);
        log.info("JSON DataFrame count: {}", jsonDF.count());

        // Show schema
        jsonDF.printSchema();

        // Test query on JSON data
        Dataset<Row> filtered = jsonDF.filter("age > 30");
        long count = filtered.count();
        assertTrue(count > 0);
        log.info("JSON filtered count: {}", count);
    }

    @Test
    void testUDFRegistration() {
        log.info("Testing User Defined Functions");

        // Register a simple UDF
        spark.udf().register("toUpperCase", (String s) -> s.toUpperCase(), DataTypes.StringType);

        List<Person> people = Arrays.asList(
                new Person("alice", 25, "new york"),
                new Person("bob", 30, "san francisco")
        );

        Dataset<Row> df = spark.createDataFrame(people, Person.class);
        df.createOrReplaceTempView("people");

        Dataset<Row> result = spark.sql("SELECT toUpperCase(name) as upper_name FROM people");
        List<Row> rows = result.collectAsList();

        assertEquals("ALICE", rows.get(0).getString(0));
        assertEquals("BOB", rows.get(1).getString(0));
        log.info("UDF test passed, results: {}", rows);
    }

    @Test
    void testBasicAggregations() {
        log.info("Testing basic aggregations");

        List<Sales> salesData = Arrays.asList(
                new Sales("Alice", "2023-01", 1000),
                new Sales("Alice", "2023-02", 1200),
                new Sales("Bob", "2023-01", 800),
                new Sales("Bob", "2023-02", 900),
                new Sales("Alice", "2023-03", 1100)
        );

        Dataset<Row> df = spark.createDataFrame(salesData, Sales.class);

        // Test basic aggregations
        Dataset<Row> totalByPerson = df.groupBy("name").sum("amount");
        long count = totalByPerson.count();

        assertEquals(2, count);
        log.info("Basic aggregation test completed");
    }

    @Test
    void testJoins() {
        log.info("Testing DataFrame joins");

        List<Person> people = Arrays.asList(
                new Person("Alice", 25, "New York"),
                new Person("Bob", 30, "San Francisco"),
                new Person("Charlie", 35, "Chicago")
        );

        List<Department> departments = Arrays.asList(
                new Department("Engineering", "Alice"),
                new Department("Marketing", "Bob"),
                new Department("Sales", "Diana")
        );

        Dataset<Row> peopleDF = spark.createDataFrame(people, Person.class);
        Dataset<Row> deptDF = spark.createDataFrame(departments, Department.class);

        // Inner join
        Dataset<Row> innerJoin = peopleDF.join(deptDF, peopleDF.col("name").equalTo(deptDF.col("employee")), "inner");
        assertEquals(2, innerJoin.count());
        log.info("Inner join count: {}", innerJoin.count());

        // Left join
        Dataset<Row> leftJoin = peopleDF.join(deptDF, peopleDF.col("name").equalTo(deptDF.col("employee")), "left");
        assertEquals(3, leftJoin.count());
        log.info("Left join count: {}", leftJoin.count());
    }

    @Test
    void testCreateDataFrameFromSchema() {
        log.info("Testing DataFrame creation from schema");

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("value", DataTypes.DoubleType, false)
        });

        List<Row> data = Arrays.asList(
                RowFactory.create(1, "Item1", 10.5),
                RowFactory.create(2, "Item2", 20.3),
                RowFactory.create(3, "Item3", 15.7)
        );

        Dataset<Row> df = spark.createDataFrame(data, schema);

        assertEquals(3, df.count());
        assertEquals("Item1", df.select("name").first().getString(0));
        log.info("DataFrame created from schema successfully");
    }

    @Test
    void testPivotOperations() {
        log.info("Testing pivot operations");

        List<Sales> salesData = Arrays.asList(
                new Sales("Alice", "Q1", 1000),
                new Sales("Alice", "Q2", 1200),
                new Sales("Bob", "Q1", 800),
                new Sales("Bob", "Q2", 900),
                new Sales("Alice", "Q3", 1100)
        );

        Dataset<Row> df = spark.createDataFrame(salesData, Sales.class);

        Dataset<Row> pivoted = df.groupBy("name")
                .pivot("month")
                .sum("amount");

        pivoted.show();
        assertTrue(pivoted.count() > 0);
        log.info("Pivot operation completed");
    }

    // Bean classes for testing
    public static class Person {
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

    public static class Department {
        private String department;
        private String employee;

        public Department() {}

        public Department(String department, String employee) {
            this.department = department;
            this.employee = employee;
        }

        // Getters and setters
        public String getDepartment() { return department; }
        public void setDepartment(String department) { this.department = department; }
        public String getEmployee() { return employee; }
        public void setEmployee(String employee) { this.employee = employee; }
    }

    public static class Sales {
        private String name;
        private String month;
        private int amount;

        public Sales() {}

        public Sales(String name, String month, int amount) {
            this.name = name;
            this.month = month;
            this.amount = amount;
        }

        // Getters and setters
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getMonth() { return month; }
        public void setMonth(String month) { this.month = month; }
        public int getAmount() { return amount; }
        public void setAmount(int amount) { this.amount = amount; }
    }
}