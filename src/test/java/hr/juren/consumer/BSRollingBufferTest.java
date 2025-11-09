package hr.juren.consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BSRollingBufferTest {

    private static class TestValue implements TimeStamped {
        private final long ts;
        private final int val;

        TestValue(long ts, int val) {
            this.ts = ts;
            this.val = val;
        }

        @Override
        public long timestamp() {
            return ts;
        }

        public int value() {
            return val;
        }
    }

    private static class TestBucket implements Bucket<TestValue> {
        private long ts;
        private int sum;

        @Override
        public void reset(TestValue value) {
            ts = value.timestamp();
            sum = value.value();
        }

        @Override
        public void aggregate(TestValue value) {
            sum += value.value();
        }

        @Override
        public long timestamp() {
            return ts;
        }

        public int sum() {
            return sum;
        }
    }

    private Supplier<TestBucket> bucketSupplier;
    private BSRollingBuffer.BSBuffer<TestBucket, TestValue> buffer;

    @BeforeEach
    void setup() {
        bucketSupplier = TestBucket::new;
        buffer = new BSRollingBuffer.BSBuffer<>(5, 3, 1000, bucketSupplier);
    }

    // ------------------------------------------------------------------------
    // Tests for basic period handling
    // ------------------------------------------------------------------------

    @Test
    void shouldAggregateValuesWithinSamePeriod() {
        buffer.update(new TestValue(1000, 2));
        buffer.update(new TestValue(1500, 3));

        Iterator<TestBucket> it = buffer.iterator(0);
        assertThat(it).toIterable()
                .hasSize(1)
                .first()
                .extracting(TestBucket::sum)
                .isEqualTo(5);
    }

    @Test
    void shouldAdvanceToNextBucketOnNewPeriod() {
        buffer.update(new TestValue(1000, 2));   // period 1
        buffer.update(new TestValue(2200, 3));   // period 2
        buffer.update(new TestValue(2500, 4));   // same period 3

        Iterator<TestBucket> it = buffer.iterator(0);
        assertThat(it).toIterable()
                .hasSize(2);

        List<TestBucket> list = new ArrayList<>();
        buffer.iterator(0).forEachRemaining(list::add);
        assertThat(list.get(0).sum()).isEqualTo(2);
        assertThat(list.get(1).sum()).isEqualTo(7); // 3 + 4
    }

    @Test
    void shouldRejectOutOfOrderTimestamps() {
        buffer.update(new TestValue(1000, 1));
        assertThatThrownBy(() -> buffer.update(new TestValue(500, 2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("strictly increasing");
    }

    // ------------------------------------------------------------------------
    // Tests for circular buffer behavior
    // ------------------------------------------------------------------------

    @Test
    void shouldRotateAfterMaxBuckets() {
        long ts = 0;
        for (int i = 0; i < 6; i++) {
            buffer.update(new TestValue(ts += 1000, i + 1));
        }

        Iterator<TestBucket> it = buffer.iterator(0);
        List<TestBucket> list = new ArrayList<>();
        it.forEachRemaining(list::add);

        // We used 6 periods with maxBuckets=5, so oldest was overwritten
        assertThat(list).hasSize(3);
        assertThat(list.get(0).sum()).isEqualTo(4); // the last 3 visible buckets: 4,5,6
        assertThat(list.get(2).sum()).isEqualTo(6);
    }

    @Test
    void shouldExposeOnlyExposedBuckets() {
        long ts = 0;
        for (int i = 0; i < 5; i++) {
            buffer.update(new TestValue(ts += 1000, i + 1));
        }

        Iterator<TestBucket> it = buffer.iterator(0);
        List<TestBucket> list = new ArrayList<>();
        it.forEachRemaining(list::add);

        // ExposedBuckets=3 means only last 3 are visible
        assertThat(list).hasSize(3);
        assertThat(list.get(0).sum()).isEqualTo(3);
        assertThat(list.get(2).sum()).isEqualTo(5);
    }

    // ------------------------------------------------------------------------
    // Tests for iterator(startTimestamp)
    // ------------------------------------------------------------------------

    @Test
    void shouldStartFromTimestampWithinVisibleRange() {
        long ts = 0;
        for (int i = 0; i < 5; i++) {
            buffer.update(new TestValue(ts += 1000, i + 1));
        }

        // Only 3 buckets visible: timestamps 3000, 4000, 5000
        Iterator<TestBucket> it = buffer.iterator(4000);

        List<Long> timestamps = new ArrayList<>();
        it.forEachRemaining(b -> timestamps.add(b.timestamp()));

        assertThat(timestamps).containsExactly(4000L, 5000L);
    }

    @Test
    void shouldReturnEmptyIteratorForTooHighStartTimestamp() {
        long ts = 0;
        for (int i = 0; i < 5; i++) {
            buffer.update(new TestValue(ts += 1000, i + 1));
        }

        Iterator<TestBucket> it = buffer.iterator(6000);
        assertThat(it.hasNext()).isFalse();
    }

    // ------------------------------------------------------------------------
    // Tests for AggregableRollingBuffer wrapper
    // ------------------------------------------------------------------------

    @Test
    void aggregableRollingBufferShouldCreateBuffersPerKey() {
        var agg = new AggregableRollingBuffer<String, TestBucket, TestValue>(bucketSupplier, 5, 3, 1000);

        agg.put("A", new TestValue(1000, 5));
        agg.put("B", new TestValue(2000, 2));
        agg.put("A", new TestValue(1500, 1));

        Iterator<TestBucket> itA = agg.iterator("A", 0);
        Iterator<TestBucket> itB = agg.iterator("B", 0);

        assertThat(itA).toIterable().hasSize(1).first().extracting(TestBucket::sum).isEqualTo(6);
        assertThat(itB).toIterable().hasSize(1).first().extracting(TestBucket::sum).isEqualTo(2);
    }

    @Test
    void aggregableRollingBufferShouldReturnEmptyIteratorForMissingKey() {
        var agg = new AggregableRollingBuffer<String, TestBucket, TestValue>(bucketSupplier, 5, 3, 1000);
        assertThat(agg.iterator("nonexistent", 0))
                .isSameAs(Collections.emptyIterator());
    }
}