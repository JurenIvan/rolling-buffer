package hr.juren.consumer;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AggregableRollingLinearBufferAggregationTest {

    record TestValue(long timestamp, int amount) implements TimeStamped {}

    static class SummingBucket implements Bucket<TestValue> {
        long ts = 0;
        int sum = 0;
        int resets = 0;
        int aggregates = 0;

        @Override public long timestamp() { return ts; }
        @Override public void reset(TestValue v) {
            ts = v.timestamp();
            sum = v.amount();
            resets++;
        }
        @Override public void aggregate(TestValue v) {
            sum += v.amount();
            aggregates++;
        }

        @Override
        public String toString() {
            return "SummingBucket{" +
                    "ts=" + ts +
                    ", sum=" + sum +
                    ", resets=" + resets +
                    ", aggregates=" + aggregates +
                    '}';
        }
    }

    private AggregableRollingBuffer<String, SummingBucket, TestValue> newBuffer() {
        return new AggregableRollingBuffer<>(SummingBucket::new, 15, 10, 1000);
    }

    @Test
    void aggregatesMultipleValuesInSamePeriod() {
        var buffer = newBuffer();
        buffer.put("A", new TestValue(1000, 10));
        buffer.put("A", new TestValue(1200, 5));
        buffer.put("A", new TestValue(1800, 2));

        var it = buffer.iterator("A", 0);
        var bucket = it.next();
        assertThat(bucket.sum).isEqualTo(17);
        assertThat(bucket.resets).isEqualTo(1);
        assertThat(bucket.aggregates).isEqualTo(2);
    }

    @Test
    void resetsWhenPeriodAdvances() {
        var buffer = newBuffer();
        buffer.put("A", new TestValue(1000, 10));
        buffer.put("A", new TestValue(2000, 7)); // new period
        buffer.put("A", new TestValue(2500, 3));

        var buckets = collect(buffer.iterator("A", 0));
        assertThat(buckets).hasSize(2);
        assertThat(buckets.get(0).sum).isEqualTo(10);
        assertThat(buckets.get(1).sum).isEqualTo(10); // 7 + 3
        assertThat(buckets.get(0).resets).isEqualTo(1);
        assertThat(buckets.get(1).aggregates).isEqualTo(1);
    }

    @Test
    void aggregationAndResetMixedCorrectly() {
        var buffer = newBuffer();
        // same period
        buffer.put("A", new TestValue(1500, 1));
        buffer.put("A", new TestValue(1900, 2));
        // next period
        buffer.put("A", new TestValue(2100, 3));
        buffer.put("A", new TestValue(2500, 4));

        var buckets = collect(buffer.iterator("A", 0));
        assertThat(buckets).hasSize(2);
        assertThat(buckets.get(0).sum).isEqualTo(3); // 1 + 2
        assertThat(buckets.get(0).resets).isEqualTo(1);
        assertThat(buckets.get(0).aggregates).isEqualTo(1);
        assertThat(buckets.get(1).sum).isEqualTo(7); // 3 + 4
        assertThat(buckets.get(1).resets).isEqualTo(1);
        assertThat(buckets.get(1).aggregates).isEqualTo(1);
    }

    @Test
    void outdatedValuesIgnoredEvenForAggregation() {
        var buffer = newBuffer();
        buffer.put("A", new TestValue(100_000, 5));
        assertThatThrownBy(()->buffer.put("A", new TestValue(1_000, 99)))
                .hasMessage("Timestamps must be strictly increasing");

        var buckets = collect(buffer.iterator("A", 0));
        assertThat(buckets).hasSize(1);
        assertThat(buckets.get(0).sum).isEqualTo(5);
        assertThat(buckets.get(0).aggregates).isEqualTo(0);
    }

    private static List<SummingBucket> collect(Iterator<SummingBucket> it) {
        var res = new ArrayList<SummingBucket>();
        while (it.hasNext()) res.add(it.next());
        return res;
    }
}