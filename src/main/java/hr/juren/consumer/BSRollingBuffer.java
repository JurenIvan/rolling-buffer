package hr.juren.consumer;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

public final class BSRollingBuffer<K, B extends Bucket<V>, V extends TimeStamped> {

    private final Lock lock = new ReentrantLock();
    private final Map<K, BSBuffer<B, V>> buffers = new HashMap<>();
    private final Supplier<B> bucketSupplier;
    private final int maxBuckets;
    private final int exposedBuckets;
    private final int periodMillis;

    public BSRollingBuffer(Supplier<B> bucketSupplier,
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

    public static final class BSBuffer<B extends Bucket<V>, V extends TimeStamped> {

        private final int maxBuckets;
        private final int exposedBuckets;
        private final long periodMillis;
        private final B[] buckets;

        private volatile int writeIndex = 0; // points to the most recent bucket

        @SuppressWarnings("unchecked")
        public BSBuffer(int maxBuckets, int exposedBuckets, long periodMillis, Supplier<B> bucketSupplier) {
            if (exposedBuckets > maxBuckets)
                throw new IllegalArgumentException("exposedBuckets <= maxBuckets required");
            this.maxBuckets = maxBuckets;
            this.exposedBuckets = exposedBuckets;
            this.periodMillis = periodMillis;
            this.buckets = (B[]) new Bucket[maxBuckets];
            Arrays.setAll(this.buckets, i -> bucketSupplier.get());
        }

        public void update(V value) {
            long valueTs = value.timestamp();

            long lastTs = buckets[writeIndex].timestamp();
            if (valueTs <= lastTs)
                throw new IllegalArgumentException("Timestamps must be strictly increasing");

            long currentPeriod = periodOf(valueTs);
            long lastPeriod = periodOf(lastTs);

            if (currentPeriod != lastPeriod) {
                // advance to next bucket
                writeIndex = writeIndex + 1 == maxBuckets ? 0 : writeIndex + 1;
                buckets[writeIndex].reset(value);
            } else {
                // same period, aggregate
                buckets[writeIndex].aggregate(value);
            }
        }

        public Iterator<B> iterator(long startTimestamp) {
            int newestIndex = writeIndex;
            int visibleCount = Math.min(exposedBuckets, maxBuckets);
            int oldestIndex = (newestIndex - (visibleCount - 1) + maxBuckets) % maxBuckets;

            // Logical binary search over circular buffer
            int low = 0;
            int high = visibleCount - 1;

            while (low <= high) {
                int mid = (low + high) >>> 1;
                int idx = (oldestIndex + mid) % maxBuckets;
                long ts = buckets[idx].timestamp();

                if (ts == 0 || ts < startTimestamp) {
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }

            // After search: low = first element with ts > 0 and ts >= startTimestamp (or == visibleCount if none)
            int startOffset = Math.min(low, exposedBuckets);
            final int startIdx = (oldestIndex + startOffset) % maxBuckets;
            final int endIdx = (newestIndex + 1) % maxBuckets;

            return new Iterator<>() {
                int i = startIdx;

                @Override
                public boolean hasNext() {
                    return i != endIdx;
                }

                @Override
                public B next() {
                    if (i == endIdx)
                        throw new NoSuchElementException();
                    B b = buckets[i];
                    i = (i + 1) >= maxBuckets ? 0 : i + 1;
                    return b;
                }
            };
        }

        private long periodOf(long timestamp) {
            return Math.floorDiv(timestamp, periodMillis);
        }
    }
}
