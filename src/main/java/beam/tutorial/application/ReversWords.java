package beam.tutorial.application;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

public class ReversWords extends PTransform<PCollection<String>,PCollection<String>> {
    private String fileTextRev="";

    public static class ReverseWord extends DoFn<String, String>{
        @ProcessElement
        public void processWord(@Element String word, OutputReceiver<String> out){
            out.output(WordUtils.reverseIt(word));
        }
    }

    @Override
    public PCollection<String> expand(PCollection<String> input) {
        return input
                .apply(MapElements.into(TypeDescriptors.lists(TypeDescriptors.strings())).via(fileText-> Arrays.asList(fileText.split(" "))))
                .apply(Flatten.<String>iterables())
                .apply(ParDo.of(new ReverseWord()))
                .apply(Combine.globally((word,word1)->word.concat(" "+word1)));
    }

}
