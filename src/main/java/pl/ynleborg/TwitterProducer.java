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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private static Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    private List<String> terms = Lists.newArrayList("mongodb", "java", "starwars");

    public static void main(String[] args) throws InterruptedException {
        Properties prop = new Properties();
        try (InputStream input = TwitterProducer.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                logger.error("Sorry, unable to find config.properties");
                return;
            }
            prop.load(input);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        new TwitterProducer().run(prop);
    }

    private void run(Properties prop) throws InterruptedException {
        Client client = null;
        try {
            logger.info("START");
            BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
            client = createTwitterClient(msgQueue, prop);
            client.connect();

            while (!client.isDone()) {
                String msg = msgQueue.poll(5, TimeUnit.SECONDS);
                if (msg != null) {
                    logger.info(msg);
                }
            }
        } finally {
            if (client != null) {
                client.stop();
            }
        }
        logger.info("STOP");
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue, Properties prop) {

        Hosts streamHost = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(terms);
        Authentication auth = new OAuth1(
                prop.getProperty("consumerKey"),
                prop.getProperty("consumerSecret"),
                prop.getProperty("token"),
                prop.getProperty("secret"));

        ClientBuilder builder = new ClientBuilder()
                .name("Jugademy2020")
                .hosts(streamHost)
                .authentication(auth)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }
}