package beam.tutorial.application;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

import javax.xml.soap.Text;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class App {

   public interface CountWordsOptions extends PipelineOptions {
        @Description("Input file location")
        @Default.String("input.txt")
        String getInput();
        void setInput(String pattern);

        @Description("Output File Words")
        @Default.String("output/OUT-WORDS.txt")
        String getOutputWords();
        void setOutputWords(String pattern);

        @Description("Output File Words Count")
        @Default.String("output/OUT-WORDS-COUNT.txt")
        String getOutputWordsCount();
        void setOutputWordsCount(String pattern);

       @Description("Output File Words Count")
       @Default.String("output/OUT-WORDS-REVERSE.txt")
       String getOutputWordsReverse();
       void setOutputWordsReverse(String pattern);
    }

    public static void main(String[] args) {
        CountWordsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CountWordsOptions.class);
        Pipeline pipeline = Pipeline.create();
        PCollection<String> initialOutput = pipeline.apply("Read from file", TextIO.read().from(options.getInput()));
        PCollection<String> finalOutput =
                initialOutput.apply(new WordCount())
                .apply(MapElements.into(TypeDescriptors.strings()).via(kv->String.format("%s -> %s",kv.getKey(),kv.getValue())));
        finalOutput.apply(TextIO.write().to(options.getOutputWordsCount()).withNumShards(1));
        finalOutput.apply(MapElements.into(TypeDescriptors.strings()).via(kv->kv.split(" ")[0]))
                .apply(TextIO.write().to(options.getOutputWords()).withNumShards(1));

        initialOutput
                .apply(new ReversWords())
                .apply(TextIO.write().to(options.getOutputWordsReverse()).withNumShards(1));




        pipeline.run().waitUntilFinish(Duration.millis(5000));
        System.out.println("Pipeline has finished");
    }
}
