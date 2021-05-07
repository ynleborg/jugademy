package pl.ynleborg.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.twitter.hbc.core.Client;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import pl.ynleborg.Producer;

import javax.annotation.PostConstruct;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class MongoProducer extends Producer {

    private static final Logger LOG = LoggerFactory.getLogger(MongoProducer.class.getName());

    @Value("${mongo.user}")
    private String user;

    @Value("${mongo.password}")
    private String password;

    @PostConstruct
    public void run() throws InterruptedException {
        MongoClient mongoClient = null;
        Client client = null;
        try {
            LOG.info("START");
            BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
            client = createTwitterClient(msgQueue);
            client.connect();
            mongoClient = MongoClients.create(String.format("mongodb+srv://%s:%s@cluster0-x1pch.mongodb.net/test?retryWrites=true&w=majority",
                    user,
                    password));
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

}
