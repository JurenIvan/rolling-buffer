package hr.juren.consumer;

public interface Bucket<V extends TimeStamped> {
    long timestamp();

    void reset(V value);    // called when a new period starts

    void aggregate(V value); // called when same-period value arrives
}
