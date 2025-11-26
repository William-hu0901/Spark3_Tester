# Spark2 Test Project

This is a comprehensive Apache Spark functionality test project written in Java 21, covering the main functional modules of Spark3.

## Project Structure

```
src/test/java/org/daodao/
├── SparkCoreTest.java          # Spark Core RDD operations tests
├── SparkSQLTest.java           # Spark SQL DataFrame operations tests
├── SparkStreamingTest.java     # Spark Streaming stream processing tests
├── SparkMLlibTest.java        # Spark MLlib machine learning tests
├── SparkGraphXTest.java        # Spark GraphX graph computing tests
├── SparkIntegrationTest.java   # Spark integration tests
├── SparkPerformanceTest.java   # Spark performance tests
├── GraphDegreesAnalyzerTest.java # Graph degree analysis tests
└── TestSuite.java              # Test suite

src/test/resources/
├── logback-test.xml            # Logging configuration
├── test-data.txt               # Test data file
└── people.json                 # JSON test data
```

## Feature Coverage

### 1. Spark Core (SparkCoreTest.java)
- RDD creation and basic operations
- Transformation operations: map, filter, flatMap
- Action operations: count, collect, reduce, first, take
- Key-value pair operations: reduceByKey, groupByKey, join
- Caching and persistence operations
- File read/write operations

### 2. Spark SQL (SparkSQLTest.java)
- DataFrame creation and operations
- SQL queries and aggregations
- User Defined Functions (UDF)
- Window functions
- Join operations
- JSON file processing
- Data pivot operations

### 3. Spark Streaming (SparkStreamingTest.java)
- Basic stream processing
- Stream transformation operations
- Window operations
- State management
- Transform operations

### 4. Spark MLlib (SparkMLlibTest.java)
- Linear regression
- Logistic regression
- ML Pipeline
- Feature engineering
- Model saving and loading
- Feature assembler

### 5. Spark GraphX (SparkGraphXTest.java)
- Graph creation and basic operations
- Graph transformations
- Graph aggregations
- Connected components
- PageRank algorithm
- Triangle counting
- Subgraph operations

### 6. Graph Degree Analysis (GraphDegreesAnalyzerTest.java)
- Graph degree calculation using Spark SQL
- In-degree, out-degree, and total degree statistics
- Graph degree statistics computation
- Empty graph and single node graph tests
- DataFrame API graph operations tests

### 7. Integration Tests (SparkIntegrationTest.java)
- RDD to DataFrame conversion
- Complex data processing pipelines
- Mock testing
- Multiple data format processing
- Performance optimization
- Error handling
- Broadcast variables

### 8. Performance Tests (SparkPerformanceTest.java)
- Large dataset processing
- Caching performance
- Parallel processing
- Memory efficiency
- Join performance
- Optimization strategies
- Broadcast variable performance

## Technology Stack

- **Java**: 21
- **Spark**: 3.5.7
- **Scala**: 2.12.18
- **JUnit**: 5.10.0
- **JUnit Platform Suite**: 1.10.1
- **Mockito**: 5.7.0
- **Lombok**: 1.18.30
- **SLF4J**: 2.0.9
- **Logback**: 1.4.11
- **Jackson**: 2.15.2
- **GraphFrames**: 0.9.0-spark3.5
- **Hadoop**: 3.3.6

## Running Tests

### Run All Tests
```bash
mvn clean test
```

### Run Specific Test Class
```bash
mvn test -Dtest=SparkCoreTest
```

### Run Test Suite
```bash
mvn test -Dtest=TestSuite
```

### Run Performance Tests
```bash
mvn test -Dtest=SparkPerformanceTest
```

### Run Graph Degree Analysis Tests
```bash
mvn test -Dtest=GraphDegreesAnalyzerTest
```

## Configuration

### Maven Configuration
- Compiled with Java 21
- Configured all necessary Spark dependencies
- Includes test dependencies and plugins
- Supports JUnit Platform Suite
- Configured JVM arguments for Java 21 compatibility

### Logging Configuration
- Uses Logback as logging implementation
- Configured console output
- Spark log level set to WARN
- Project log level set to DEBUG

### Test Data
- Provides text and JSON format test data
- Data files located in `src/test/resources/` directory

## Important Notes

1. **Memory Configuration**: Tests use local mode, recommend allocating at least 2GB memory
2. **Parallelism**: Uses all available local cores by default
3. **Test Isolation**: Each test class has independent setup and teardown
4. **Performance Tests**: Some performance tests may require longer time to complete
5. **GraphX Tests**: Require Scala support, ensure Scala libraries are properly configured
6. **Java 21 Compatibility**: JVM arguments are configured for Java 21 module system compatibility

## Extension Suggestions

1. Add more Spark functionality test cases
2. Integrate CI/CD pipelines
3. Add performance benchmarking tests
4. Support testing with different Spark versions
5. Add more data source connection tests
6. Add Spark Structured Streaming tests
7. Include GraphFrames integration tests

## Troubleshooting

### Common Issues
1. **Insufficient Memory**: Increase JVM heap memory settings
2. **Scala Version Conflicts**: Ensure Scala version is compatible with Spark
3. **Test Timeouts**: Adjust test timeout settings
4. **Dependency Conflicts**: Check Maven dependency tree
5. **Java 21 Module System**: Ensure proper JVM arguments are configured

### Debugging Tips
1. Check test log output
2. Use Spark UI to monitor job execution
3. Verify test data files exist
4. Confirm Maven dependencies are correctly downloaded
5. Check GraphFrames compatibility if graph tests fail