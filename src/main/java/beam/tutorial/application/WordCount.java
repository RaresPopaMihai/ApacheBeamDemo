package beam.tutorial.application;

import io.vavr.API;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

import static io.vavr.API.*;

public class WordCount extends PTransform<PCollection<String>,PCollection<KV<String,Long>>> {
    @Override
    public PCollection<KV<String, Long>> expand(PCollection<String> input) {
        return input.apply(Filter.by(line->!line.equals("")))
             .apply(MapElements.into(TypeDescriptors.lists(TypeDescriptors.strings())).via(line -> Arrays.asList(line.split(" "))))
             .apply(Flatten.iterables())
             .apply(MapElements.into(TypeDescriptors.strings())
                     .via(WordUtils::getOnlyWord))
             .apply(Count.perElement());
    }
}
