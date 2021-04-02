package beam.tutorial.tweet;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.io.Serializable;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Tweet implements Serializable {
    @JsonProperty("full text")
    private String fullText;
    private String lang;
    private User user;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public class User implements Serializable{
        private Long id;
        private String name;
        @JsonProperty("screen name")
        private String screenName;
    }

}
