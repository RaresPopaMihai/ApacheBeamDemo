package beam.test;



import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;
import java.util.List;

public class CountWords extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {

    public static class ExtractWords extends SimpleFunction<String, List<String>> {
        @Override
        public List<String> apply(String input) {
            return Arrays.asList(input.split(""));
        }
    }

    @Override
    public PCollection<KV<String, Long>> expand(PCollection<String> input) {
        return input
                //.apply(MapElements.via(new ExtractWords()))
                .apply(MapElements.into(TypeDescriptors.lists(TypeDescriptors.strings())).via(s->Arrays.asList(s.split(" "))))
                .apply(Flatten.<String>iterables())
                .apply(Count.<String>perElement());
    }
}
