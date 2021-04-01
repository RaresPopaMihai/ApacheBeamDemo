package beam.unittest;

import beam.test.CountWords;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


import java.io.File;
import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertThat;


public class PipelineTest {

    public static final String[] WORDS_FROM_FILE_ARRAY = new String[]{
            "ala bala","portocala","ala din","mere pere","micsunele mere din pom","bala ala mere","mere","mere","mere"
            ,"computer","monitor","xyz"
    };
    public static final String[] args = new String[] {
            "--inputFiles=input/*.txt","--outputFiles=output/OUT"
    };

    public static Pipeline pipeline;

    public static String MY_FILE = "myfile.txt";

    @BeforeClass
    public static void writeMyFile() throws IOException {
        String fileContent= Stream.of(WORDS_FROM_FILE_ARRAY).collect(Collectors.joining("\n"));
        FileUtils.writeStringToFile(new File(MY_FILE),fileContent,"UTF-8");
        pipeline = Pipeline.create();
    }
    @AfterClass
    public static void deleteFiles() throws IOException{
     FileUtils.deleteQuietly(new File(MY_FILE));
    }


    @Test
    public  void testRightFromFiles(){
        PCollection<String> fileContents = pipeline.apply(TextIO.read().from(MY_FILE));
        PAssert.that(fileContents).containsInAnyOrder(
                "ala bala","portocala","ala din","mere pere","micsunele mere din pom","bala ala mere","mere","mere","mere"
                ,"computer","monitor","xyz"
        );
        pipeline.run();
    }

    @Test
    public void testCountWords(){
        PCollection<KV<String,Long>> countWordsCollection = pipeline.apply(TextIO.read().from(MY_FILE))
                .apply(new CountWords());
        PAssert.that(countWordsCollection).containsInAnyOrder(
                KV.of("xyz",1l),
                KV.of("micsunele",1l),
                KV.of("mere",6l),
                KV.of("pere",1l),
                KV.of("portocala",1l),
                KV.of("bala",2l),
                KV.of("pom",1l),
                KV.of("monitor",1l),
                KV.of("ala",Long.valueOf(3)),
                KV.of("din",2l),
                KV.of("computer",1l)
        );
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testRightEntirePipeline(){
        PCollection<String> finalCollecti0n=pipeline
                .apply(TextIO.read().from(MY_FILE))
                .apply(new CountWords())
                .apply(
                        MapElements.into(TypeDescriptors.strings())
                                .via(kv->String.format("%s: %d", kv.getKey(),kv.getValue())));
        PAssert.that(finalCollecti0n).containsInAnyOrder(
                "xyz: 1",
                "micsunele: 1",
                "portocala: 1",
                "pere: 1",
                "mere: 6",
                "bala: 2",
                "pom: 1",
                "monitor: 1",
                "ala: 3",
                "din: 2",
                "computer: 1"
        );
        pipeline.run().waitUntilFinish();
    }



}
