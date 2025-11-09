package hr.juren.consumer;

import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class TestRunr {

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
                .include(AggregableRollingBufferBenchmark.class.getSimpleName())
//                .include(RawListBenchmark.class.getSimpleName())
                .resultFormat(ResultFormatType.JSON)
                .result("jmh-result.json")
                .build();

        new Runner(options).run();
    }
}
