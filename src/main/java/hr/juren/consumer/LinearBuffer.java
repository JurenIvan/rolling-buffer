package hr.juren.consumer;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

public final class LinearBuffer<B extends Bucket<V>, V extends TimeStamped> {

    private final int maxBuckets;
    private final int exposedBuckets;
    private final long periodMillis;
    private final B[] buckets;
    private volatile long lastTimestamp = 0L;

    @SuppressWarnings("unchecked")
    LinearBuffer(int maxBuckets, int exposedBuckets, long periodMillis, Supplier<B> bucketSupplier) {
        if (exposedBuckets > maxBuckets)
            throw new IllegalArgumentException("exposedBuckets <= maxBuckets required");
        this.maxBuckets = maxBuckets;
        this.exposedBuckets = exposedBuckets;
        this.periodMillis = periodMillis;
        this.buckets = (B[]) new Bucket[maxBuckets];

        Arrays.setAll(this.buckets, i -> bucketSupplier.get());
    }

    public void update(V value) {
        long snapshotLastTs = lastTimestamp;
        long valueTs = value.timestamp();

        if (valueTs < snapshotLastTs - exposedBuckets * periodMillis) {
            // outdated value, ignore
            return;
        }

        long currentPeriod = periodOf(valueTs);
        int idx = (int) Math.floorMod(currentPeriod, (long) maxBuckets);
        B bucket = buckets[idx];

        long bucketPeriod = periodOf(bucket.timestamp());
        if (bucket.timestamp() == 0 || bucketPeriod != currentPeriod) {
            bucket.reset(value); // new period
        } else {
            bucket.aggregate(value); // same period
        }

        lastTimestamp = Math.max(snapshotLastTs, valueTs);
    }

    public Iterator<B> iterator(long startTimestamp) {
        return new LinearBufferIterator(startTimestamp);
    }

    private long periodOf(long timestamp) {
        return Math.floorDiv(timestamp, periodMillis);
    }

    private final class LinearBufferIterator implements Iterator<B> {
        private final long lastPeriod;
        private int idx;
        private long nextPeriod;
        private B next;

        LinearBufferIterator(long startTimestamp) {
            this.lastPeriod = periodOf(lastTimestamp);

            long earliestPeriod = lastPeriod - (exposedBuckets - 1L);
            long startPeriod = Math.max(earliestPeriod, periodOf(startTimestamp));

            this.idx = (int) Math.floorMod(startPeriod, (long) maxBuckets);
            this.nextPeriod = startPeriod;

            advance();
        }

        private void advance() {
            next = null;
            while (nextPeriod <= lastPeriod) {
                B candidate = buckets[idx];
                if (candidate.timestamp() != 0 &&
                        periodOf(candidate.timestamp()) == nextPeriod) {
                    next = candidate;
                    break;
                }
                idx = (idx + 1) % maxBuckets;
                nextPeriod++;
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public B next() {
            if (next == null)
                throw new NoSuchElementException();
            B result = next;
            idx = (idx + 1) % maxBuckets;
            nextPeriod++;
            advance();
            return result;
        }
    }
}