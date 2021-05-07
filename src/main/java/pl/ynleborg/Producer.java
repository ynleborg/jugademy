package pl.ynleborg;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.springframework.beans.factory.annotation.Value;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public class Producer {
    private static final List<String> TERMS = Lists.newArrayList("kafka", "java", "covid");

    @Value("${twitter.consumerKey}")
    private String consumerKey;

    @Value("${twitter.consumerSecret}")
    private String consumerSecret;

    @Value("${twitter.token}")
    private String token;

    @Value("${twitter.secret}")
    private String secret;

    protected Client createTwitterClient(BlockingQueue<String> msgQueue) {

        Hosts streamHost = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(TERMS);
        Authentication auth = new OAuth1(
                consumerKey,
                consumerSecret,
                token,
                secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Jugademy")
                .hosts(streamHost)
                .authentication(auth)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }
}
