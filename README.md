# Rolling Buffer

A high-performance, thread-safe, multi-key rolling buffer for time-windowed data aggregation and concurrent access.

## Quick Start

```java
AggregableRollingBuffer<String, MetricBucket, MetricValue> buffer =
    new AggregableRollingBuffer<>(
        MetricBucket::new,    // bucket factory
        60,                    // maxBuckets
        60,                    // exposedBuckets  
        60_000                 // periodMillis
    );

// Write
buffer.put("cpu", new MetricValue(timestamp, 42.5));

// Read
buffer.iterator("cpu", oneHourAgo).forEachRemaining(bucket -> {
    System.out.println(bucket.average());
});
```

## Architecture

**Two-level design:**

1. **AggregableRollingBuffer** - Multi-key manager (lazy creation, lock-free routing)
2. **BSBuffer** - Circular array with volatile write pointer (lock-free reads/writes per-key)

```
AggregableRollingBuffer (multi-key)
  └─ Lock (only during buffer creation)
     ├─ BSBuffer["key1"] (lock-free)
     ├─ BSBuffer["key2"] (lock-free)
     └─ BSBuffer["key3"] (lock-free)
```

## Core Features

| Feature | Details |
|---------|---------|
| **Thread-safety** | Lock-free reads; one lock per key on creation |
| **Memory** | Pre-allocated circular array; no GC during operation |
| **Timestamps** | Binary search; supports sparse data with grace zone |
| **Concurrency** | Readers iterate safely while writers advance |

## Performance Optimizations

### The Modulo Problem

**Before (slow):**
```java
nextIndex = (nextIndex + 1) % maxBuckets;  // Expensive on CPU
```

**After (3× faster):**
```java
nextIndex = (nextIndex + 1 == maxBuckets) ? 0 : nextIndex + 1;  // Branch-friendly
```

This single change improved iteration performance by **3×** — beating `ArrayList`, `ArrayDeque`, and `LinkedList`.

## Benchmarking with JMH

This project uses **Java Microbenchmark Harness (JMH)** to measure performance accurately.

### Why JMH?

JVM optimizations (JIT, inlining, dead code elimination) make simple benchmarks unreliable. JMH eliminates common pitfalls:

- **Dead code elimination** - Prevents JVM from skipping your benchmark
- **JIT warmup** - Allows JIT compiler to optimize before measurement
- **Statistical rigor** - Runs multiple iterations and reports confidence intervals

### Running Benchmarks

```bash
mvn clean install
java -jar target/benchmarks.jar

# Run specific benchmark
java -jar target/benchmarks.jar RawListBenchmark.arrayListIteration

# With output redirection
java -jar target/benchmarks.jar > results.txt
```

### Benchmark Structure

Located in `src/jmh/java/`:

```
AggregableRollingBufferBenchmark.rollingBufferIteration
RawListBenchmark.arrayListIteration
RawListBenchmark.arrayDequeIteration
RawListBenchmark.linkedListIteration
```

Each measures **iteration latency** across buffer sizes: 64, 256, 1024, 4096, 16384, 65536, 262144, 1048576

### Output Format

```
AggregableRollingBufferBenchmark.rollingBufferIteration  64  avgt  5  0.054 ± 0.001  us/op
                                                          └─ buffer size (elements)
                                                                └─ average time  
                                                                       └─ std dev
                                                                                  └─ microseconds per operation
```

**Columns:**
- `Benchmark` - Test name and size parameter
- `Mode` - `avgt` (average time)
- `Cnt` - Number of measurement iterations
- `Score ± Error` - Result with 95% confidence interval
- `Units` - `us/op` (microseconds per operation)

### Key Metrics (After Optimization)

| Comparison | Speedup |
|------------|---------|
| vs ArrayList | 1.3× faster |
| vs ArrayDeque | 1.4× faster |
| vs LinkedList | 2.3× faster |

**At 1M elements:** RollingBuffer = 1.1ms vs ArrayDeque = 1.6ms

## When to Use

✅ **Good fit:**
- Metrics/monitoring aggregation
- Time-windowed queries
- Multi-stream time-series
- Low GC pressure required

❌ **Not needed:**
- Single-threaded apps
- Unordered data
- Small datasets

## Project Structure

```
rolling-buffer/
├── src/main/java/
│   └── hr/juren/consumer/
│       ├── AggregableRollingBuffer.java    (multi-key manager)
│       ├── BSRollingBuffer.java            (original design)
│       ├── BSBuffer.java                   (circular buffer)
│       ├── Bucket.java                     (interface)
│       └── TimeStamped.java                (interface)
├── src/jmh/java/
│   ├── AggregableRollingBufferBenchmark.java
│   └── RawListBenchmark.java
└── pom.xml
```

## The Journey

1. ❌ Built first version; 2.5× **slower** than ArrayDeque
2. ✅ Optimized iteration (binary search); 15% improvement
3. 🎯 Found real bottleneck: modulo operator
4. 🚀 Replaced `%` with branch; **3× faster**, now beats standard collections

**Lesson:** Benchmarks don't lie — intuition does.

## References

- [JMH Official Guide](https://github.com/openjdk/jmh)
- Similar patterns: LMAX Disruptor, Chronicle Queue
- Java Memory Model: Volatile semantics for lock-free coordination