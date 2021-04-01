package beam.tutorial.application;

public class WordUtils {
    public static String reverseIt(String word){
       return new StringBuilder(word).reverse().toString();
    }

    public static String getOnlyWord(String word){
        if (word.contains(".")){
            return word.substring(0,word.indexOf("."));
        }

        else if (word.contains(","))
            return word.substring(0,word.indexOf(","));
        else
            return word;
    }
}
