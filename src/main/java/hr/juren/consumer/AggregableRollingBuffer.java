package hr.juren.consumer;

import hr.juren.consumer.BSRollingBuffer.BSBuffer;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

public final class AggregableRollingBuffer<K, B extends Bucket<V>, V extends TimeStamped> {

    private final Lock lock = new ReentrantLock();
    private final Map<K, BSBuffer<B, V>> buffers = new HashMap<>();
    private final Supplier<B> bucketSupplier;
    private final int maxBuckets;
    private final int exposedBuckets;
    private final int periodMillis;

    public AggregableRollingBuffer(Supplier<B> bucketSupplier,
                                   int maxBuckets,
                                   int exposedBuckets,
                                   int periodMillis) {
        this.bucketSupplier = Objects.requireNonNull(bucketSupplier);
        this.maxBuckets = maxBuckets;
        this.exposedBuckets = exposedBuckets;
        this.periodMillis = periodMillis;
    }

    public void put(K key, V value) {
        var buffer = buffers.get(key);
        if (buffer == null) {
            lock.lock();
            try {
                buffer = buffers.computeIfAbsent(key, k -> new BSBuffer<>(maxBuckets, exposedBuckets, periodMillis, bucketSupplier));
            } finally {
                lock.unlock();
            }
        }
        buffer.update(value);
    }

    public Iterator<B> iterator(K key, long startTimestamp) {
        var buffer = buffers.get(key);
        if (buffer == null) return Collections.emptyIterator();
        return buffer.iterator(startTimestamp);
    }
}
