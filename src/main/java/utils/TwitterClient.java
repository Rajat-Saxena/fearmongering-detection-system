package utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class TwitterClient
{
    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessTokenSecret;

    public TwitterClient() throws IOException
    {
        Properties properties = new Properties();
        InputStream input = getClass().getClassLoader().getResourceAsStream("twitterAuth.properties");
        properties.load(input);

        this.consumerKey = properties.getProperty("consumerKey");
        this.consumerSecret = properties.getProperty("consumerSecret");
        this.accessToken = properties.getProperty("accessToken");
        this.accessTokenSecret = properties.getProperty("accessTokenSecret");
    }

    public String getConsumerKey() {
        return consumerKey;
    }

    public String getConsumerSecret() {
        return consumerSecret;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public String getAccessTokenSecret() {
        return accessTokenSecret;
    }
}
