package hr.juren.consumer;

import hr.juren.consumer.AggregableRollingBufferBenchmark.TestValue;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Fork(1)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class RawListBenchmark {

    // ========================================================================
    // 1. Simple iteration over Lists and Deq
    // ========================================================================
    @State(Scope.Thread)
    public static class ArrayListIterationState {
        @Param({"64", "256", "1024", "4096", "16384", "65536", "262144", "1048576"})
        int maxBuckets;

        ArrayList<TestValue> arrayList;
        LinkedList<TestValue> linkedList;
        ArrayDeque<TestValue> deq;

        @Setup(Level.Trial)
        public void setup() {
            var now = System.currentTimeMillis();
            arrayList = new ArrayList<>(maxBuckets);
            linkedList = new LinkedList<>();
            deq = new ArrayDeque<>(maxBuckets);
            for (int i = 0; i < maxBuckets; i++) {
                arrayList.add(new TestValue(i, now - i));
                linkedList.add(new TestValue(i, now - i));
                deq.add(new TestValue(i, now - i));
            }
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void arrayListIteration(ArrayListIterationState state, Blackhole bh) {
        long sum = 0;
        for (var ts : state.arrayList) {
            sum += ts.timestamp();
        }
        bh.consume(sum);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void linkedListIteration(ArrayListIterationState state, Blackhole bh) {
        long sum = 0;
        for (var ts : state.linkedList) {
            sum += ts.timestamp();
        }
        bh.consume(sum);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void arrayDequeIteration(ArrayListIterationState state, Blackhole bh) {
        long sum = 0;
        for (var ts : state.deq) {
            sum += ts.timestamp();
        }
        bh.consume(sum);
    }

    // ========================================================================
    // 2. Head removal + tail append
    // ========================================================================
    @State(Scope.Benchmark)
    public static class ListRotateState {
        @Param({"64", "256", "1024", "4096"})
        int maxBuckets;

        ArrayList<TestValue> arrayList;
        LinkedList<TestValue> linkedList;
        ArrayDeque<TestValue> deq;
        AtomicLong timestamp;

        @Setup(Level.Trial)
        public void setup() {
            timestamp = new AtomicLong(System.currentTimeMillis());
            arrayList = new ArrayList<>(maxBuckets);
            linkedList = new LinkedList<>();
            deq = new ArrayDeque<>(maxBuckets);
            for (int i = 0; i < maxBuckets; i++) {
                arrayList.add(new TestValue(i, timestamp.get() - i));
                linkedList.add(new TestValue(i, timestamp.get() - i));
                deq.add(new TestValue(i, timestamp.get() - i));
            }
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void arrayListRotate(ListRotateState state) {
        long ts = state.timestamp.getAndAdd(1000);

        state.arrayList.removeFirst(); // O(n)
        state.arrayList.add(new TestValue((int) ts, ts));
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void deqRotate(ListRotateState state) {
        long ts = state.timestamp.getAndAdd(1000);

        state.deq.removeFirst(); // O(1)
        state.deq.add(new TestValue((int) ts, ts)); //O(1)
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void linkedListRotate(ListRotateState state) {
        long ts = state.timestamp.getAndAdd(1000);

        state.linkedList.removeFirst(); // O(1)
        state.linkedList.addLast(new TestValue((int) ts, ts)); // O(1)
    }
}