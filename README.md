# Spark Service Test Project

这是一个完整的Apache Spark功能测试项目，使用Java 21编写，涵盖了Spark的主要功能模块。

## 项目结构

```
src/test/java/org/daodao/
├── SparkCoreTest.java          # Spark Core RDD操作测试
├── SparkSQLTest.java           # Spark SQL DataFrame操作测试
├── SparkStreamingTest.java     # Spark Streaming流处理测试
├── SparkMLlibTest.java        # Spark MLlib机器学习测试
├── SparkGraphXTest.java        # Spark GraphX图计算测试
├── SparkIntegrationTest.java   # Spark集成测试
├── SparkPerformanceTest.java   # Spark性能测试
└── TestSuite.java              # 测试套件

src/test/resources/
├── logback-test.xml            # 日志配置
├── test-data.txt               # 测试数据文件
└── people.json                 # JSON测试数据
```

## 功能覆盖

### 1. Spark Core (SparkCoreTest.java)
- RDD创建和基本操作
- 转换操作：map, filter, flatMap
- 行动操作：count, collect, reduce, first, take
- 键值对操作：reduceByKey, groupByKey, join
- 缓存和持久化操作
- 文件读写操作

### 2. Spark SQL (SparkSQLTest.java)
- DataFrame创建和操作
- SQL查询和聚合
- 用户自定义函数(UDF)
- 窗口函数
- 连接操作
- JSON文件处理
- 数据透视操作

### 3. Spark Streaming (SparkStreamingTest.java)
- 基本流处理
- 流式转换操作
- 窗口操作
- 状态管理
- Transform操作

### 4. Spark MLlib (SparkMLlibTest.java)
- 线性回归
- 逻辑回归
- ML Pipeline
- 特征工程
- 模型保存和加载
- 特征组装器

### 5. Spark GraphX (SparkGraphXTest.java)
- 图创建和基本操作
- 图转换
- 图聚合
- 连通组件
- PageRank算法
- 三角计数
- 子图操作

### 6. 图度数分析 (GraphDegreesAnalyzerTest.java)
- 使用Spark SQL进行图度数计算
- 入度、出度和总度数统计
- 图度数统计信息计算
- 空图和单节点图测试
- DataFrame API图操作测试

### 7. 集成测试 (SparkIntegrationTest.java)
- RDD与DataFrame转换
- 复杂数据处理流水线
- Mock测试
- 多种数据格式处理
- 性能优化
- 错误处理
- 广播变量

### 8. 性能测试 (SparkPerformanceTest.java)
- 大数据集处理
- 缓存性能
- 并行处理
- 内存效率
- 连接性能
- 优化策略
- 广播变量性能

## 技术栈

- **Java**: 21
- **Spark**: 3.5.0
- **Scala**: 2.12
- **JUnit**: 5.10.0
- **Mockito**: 5.7.0
- **Lombok**: 1.18.30
- **SLF4J**: 2.0.9
- **Logback**: 1.4.11

## 运行测试

### 运行所有测试
```bash
mvn clean test
```

### 运行特定测试类
```bash
mvn test -Dtest=SparkCoreTest
```

### 运行测试套件
```bash
mvn test -Dtest=TestSuite
```

### 运行性能测试
```bash
mvn test -Dtest=SparkPerformanceTest
```

## 配置说明

### Maven配置
- 使用Java 21编译
- 配置了所有必要的Spark依赖
- 包含测试依赖和插件
- 支持JUnit Platform Suite

### 日志配置
- 使用Logback作为日志实现
- 配置了控制台输出
- Spark日志级别设为WARN
- 项目日志级别设为DEBUG

### 测试数据
- 提供了文本和JSON格式的测试数据
- 数据文件位于`src/test/resources/`目录

## 注意事项

1. **内存配置**: 测试使用本地模式，建议至少分配2GB内存
2. **并行度**: 默认使用本地所有可用核心
3. **测试隔离**: 每个测试类都有独立的setup和teardown
4. **性能测试**: 某些性能测试可能需要较长时间完成
5. **GraphX测试**: 需要Scala支持，确保Scala库正确配置

## 扩展建议

1. 添加更多Spark功能的测试用例
2. 集成CI/CD流水线
3. 添加性能基准测试
4. 支持不同Spark版本的测试
5. 添加更多数据源连接测试

## 故障排除

### 常见问题
1. **内存不足**: 增加JVM堆内存设置
2. **Scala版本冲突**: 确保Scala版本与Spark兼容
3. **测试超时**: 调整测试超时时间
4. **依赖冲突**: 检查Maven依赖树

### 调试建议
1. 查看测试日志输出
2. 使用Spark UI监控作业执行
3. 检查测试数据文件是否存在
4. 验证Maven依赖是否正确下载