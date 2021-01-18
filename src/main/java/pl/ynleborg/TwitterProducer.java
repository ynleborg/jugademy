package pl.ynleborg;

import com.google.common.collect.Lists;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterProducer.class.getName());

    private static final List<String> TERMS = Lists.newArrayList("mongodb", "java", "starwars");

    public void run(Properties prop) throws InterruptedException {
        MongoClient mongoClient = null;
        Client client = null;
        try {
            LOG.info("START");
            BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
            client = createTwitterClient(msgQueue, prop);
            client.connect();
            mongoClient = MongoClients.create(String.format("mongodb+srv://%s:%s@cluster0-x1pch.mongodb.net/test?retryWrites=true&w=majority",
                    prop.getProperty("mongo.user"),
                    prop.getProperty("mongo.password")));
            MongoDatabase database = mongoClient.getDatabase("jugadamy2021");
            MongoCollection<Document> tweets = database.getCollection("tweets");

            while (!client.isDone()) {
                String msg = msgQueue.poll(5, TimeUnit.SECONDS);
                if (msg != null) {
                    LOG.debug(msg);
                    tweets.insertOne(Document.parse(msg));
                }
            }
        } finally {
            if (mongoClient != null) {
                mongoClient.close();
            }
            if (client != null) {
                client.stop();
            }
        }
        LOG.info("STOP");
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue, Properties prop) {

        Hosts streamHost = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(TERMS);
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
