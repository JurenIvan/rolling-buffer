package hr.juren.consumer;

import hr.juren.consumer.BSRollingBuffer.BSBuffer;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openjdk.jmh.annotations.Mode.Throughput;

@Fork(value = 1)
@Warmup(iterations = 3, time = 1, timeUnit = SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = SECONDS)
@BenchmarkMode(Throughput)
@OutputTimeUnit(SECONDS)
public class AggregableRollingBufferBenchmark {

    // ============================================================================
    // 1. Simple iteration over RollingBuffer
    // ============================================================================
    @State(Scope.Thread)
    public static class ReadState {
        @Param({"64", "256", "1024", "4096", "16384", "65536", "262144", "1048576"})
        int maxBuckets;

        BSBuffer<TestBucket, TestValue> buffer;

        @Setup(Level.Trial)
        public void setup() {
            buffer = new BSBuffer<>(
                    maxBuckets,
                    maxBuckets - 2,
                    1000,
                    TestBucket::new
            );

            long base = 1_202_000L;
            for (int i = 0; i < maxBuckets; i++) {
                buffer.update(new TestValue(i, base + (i * 1000)));
            }
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void rollingBufferIteration(ReadState state, Blackhole bh) {
        var it = state.buffer.iterator(1_202_000L);
        long sum = 0;
        while (it.hasNext()) {
            TestBucket value = it.next();
            sum += value.sum;
        }
        bh.consume(sum);
    }

    // ============================================================================
    // 2. WRITE THROUGHPUT
    // ============================================================================

    @State(Scope.Benchmark)
    public static class WriteState {
        BSBuffer<TestBucket, TestValue> buffer;
        AtomicLong timestamp;

        @Param({"64", "256", "1024", "4096"})
        int maxBuckets;

        @Setup(Level.Trial)
        public void setup() {
            timestamp = new AtomicLong(System.currentTimeMillis());
            buffer = new BSBuffer<>(
                    maxBuckets,
                    maxBuckets - 2,
                    1000,
                    (Supplier<TestBucket>) TestBucket::new
            );
        }
    }

    @Benchmark
    @BenchmarkMode(Throughput)
    @OutputTimeUnit(SECONDS)
    public void writeThroughput(WriteState state) {
        long ts = state.timestamp.getAndAdd(1000);
        state.buffer.update(new TestValue((int) ts, ts));
    }

//    // ============================================================================
//    // 3.1 CONCURRENCY STRESS - 1 Producer + 1 Reader
//    // ============================================================================
//
//    @State(Scope.Benchmark)
//    public static class Concurrency1P1RState {
//        Buffer<TestBucket, TestValue> buffer;
//        AtomicLong producerTimestamp;
//        AtomicLong readCount;
//        AtomicLong stopProducer;
//        ExecutorService executor;
//
//        @Setup(Level.Trial)
//        public void setup() {
//            producerTimestamp = new AtomicLong(System.currentTimeMillis());
//            readCount = new AtomicLong(0);
//            stopProducer = new AtomicLong(0);
//            executor = Executors.newFixedThreadPool(1);
//
//            buffer = new Buffer<>(512, 510, 1000, TestBucket::new);
//        }
//
//        @TearDown(Level.Trial)
//        public void tearDown() {
//            stopProducer.set(1);
//            executor.shutdown();
//        }
//    }
//
//    @State(Scope.Thread)
//    public static class Concurrency1P1RWriteState {
//        AtomicLong timestamp;
//        int writeCount;
//
//        @Setup(Level.Trial)
//        public void setup() {
//            timestamp = new AtomicLong(System.currentTimeMillis());
//            writeCount = 0;
//        }
//    }
//
//    @Benchmark
//    @BenchmarkMode(Throughput)
//    @Threads(1)
//    public void concurrency1P1RWrite(Concurrency1P1RState state, Concurrency1P1RWriteState writeState) {
//        long ts = state.producerTimestamp.getAndAdd(200);
//        state.buffer.update(new TestValue(writeState.writeCount++, ts));
//    }
//
//    @Benchmark
//    @BenchmarkMode(Mode.AverageTime)
//    @OutputTimeUnit(TimeUnit.MICROSECONDS)
//    @Threads(1)
//    public void concurrency1P1RRead(Concurrency1P1RState state, Blackhole bh) {
//        var iterator = state.buffer.iterator(System.currentTimeMillis() - 500_000);
//        int count = 0;
//        while (iterator.hasNext()) {
//            var bucket = iterator.next();
//            bh.consume(bucket);
//            count++;
//        }
//        state.readCount.incrementAndGet();
//        bh.consume(count);
//    }
//
//    // ============================================================================
//    // 3.2 CONCURRENCY STRESS - 1 Producer + 5 Readers
//    // ============================================================================
//
//    @Benchmark
//    @BenchmarkMode(Throughput)
//    @OutputTimeUnit(SECONDS)
//    @Threads(1)
//    public void concurrency1P5RMaxWrite(Concurrency1P1RState state, Concurrency1P1RWriteState writeState) {
//        long ts = state.producerTimestamp.getAndAdd(200);
//        state.buffer.update(new TestValue(writeState.writeCount++, ts));
//    }
//
//    @Benchmark
//    @BenchmarkMode(Mode.AverageTime)
//    @OutputTimeUnit(TimeUnit.MICROSECONDS)
//    @Threads(5)
//    public void concurrency1P5RRead(Concurrency1P1RState state, Blackhole bh) {
//        var iterator = state.buffer.iterator(System.currentTimeMillis() - 500_000);
//        int count = 0;
//        while (iterator.hasNext()) {
//            var bucket = iterator.next();
//            bh.consume(bucket);
//            count++;
//        }
//        bh.consume(count);
//    }
//
//    // ============================================================================
//    // 3.3 CONCURRENCY STRESS - 1 Producer + 10 Readers (max throughput)
//    // ============================================================================
//
//    @Benchmark
//    @BenchmarkMode(Throughput)
//    @OutputTimeUnit(SECONDS)
//    @Threads(1)
//    public void concurrency1P10RMaxWrite(Concurrency1P1RState state, Concurrency1P1RWriteState writeState) {
//        long ts = state.producerTimestamp.getAndAdd(200);
//        state.buffer.update(new TestValue(writeState.writeCount++, ts));
//    }
//
//    @Benchmark
//    @BenchmarkMode(Mode.AverageTime)
//    @OutputTimeUnit(TimeUnit.MICROSECONDS)
//    @Threads(10)
//    public void concurrency1P10RRead(Concurrency1P1RState state, Blackhole bh) {
//        var iterator = state.buffer.iterator(System.currentTimeMillis() - 500_000);
//        int count = 0;
//        while (iterator.hasNext()) {
//            var bucket = iterator.next();
//            bh.consume(bucket);
//            count++;
//        }
//        bh.consume(count);
//    }

    // ============================================================================
    // Test Data Classes
    // ============================================================================

    public static class TestValue implements TimeStamped {
        private final int id;
        private final long ts;

        public TestValue(int id, long ts) {
            this.id = id;
            this.ts = ts;
        }

        @Override
        public long timestamp() {
            return ts;
        }
    }

    public static class TestBucket implements Bucket<TestValue> {
        private long timestamp = 0;
        private long sum = 0;
        private long count = 0;

        @Override
        public long timestamp() {
            return timestamp;
        }

        @Override
        public void reset(TestValue value) {
            this.timestamp = value.timestamp();
            this.sum = value.id;
            this.count = 1;
        }

        @Override
        public void aggregate(TestValue value) {
            this.sum += value.id;
            this.count++;
        }

        public long getSum() {
            return sum;
        }

        public long getCount() {
            return count;
        }
    }
}