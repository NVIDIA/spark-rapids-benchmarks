# Benchmark Listener - JVM Listener for Spark

## Binary Compatibility

This allows a single JAR compiled with Scala 2.12 to run in both:
- Spark 3.x with Scala 2.12
- Spark 3.5+ with Scala 2.13
- Spark 4.x with Scala 2.13

## Building

### Build with Maven

```bash
mvn clean package
```

This produces: `target/benchmark-listener-1.0-SNAPSHOT.jar`

### Build Requirements

- Java 8+
- Maven 3.6+
- Scala is managed by Maven (no local Scala installation needed)
