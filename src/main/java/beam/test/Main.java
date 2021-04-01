package beam.test;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptors;

public class Main {
    public interface WordCountOptions extends PipelineOptions{
        @Default.String("input/file*.txt")
        @Description("Input file name pattern")
        String getInputFiles();
        void setInputFiles(String pattern);
        String getOutputFiles();
        void setOutputFiles(String path);


    }

    public static void main(String[] args) {
        WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WordCountOptions.class);

        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply(TextIO.read().from(options.getInputFiles()))
                .apply(new CountWords())
                .apply(
                        MapElements.into(TypeDescriptors.strings())
                        .via(kv->String.format("%s: %d", kv.getKey(),kv.getValue())))
                .apply(TextIO.write().to(options.getOutputFiles()).withNumShards(1));

        pipeline.apply(Create.of(1)).apply(ParDo.of(new DoFn<Integer, Integer>() {
            @ProcessElement
            public void processElement(@Element Integer el, ProcessContext ctx) {
                System.out.println("El " + el + " ctx.element " + ctx.element());
                ctx.output(22);
            }
        }));
        pipeline.run().waitUntilFinish();
    }
}
