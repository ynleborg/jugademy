package pl.ynleborg.kafka;

import com.twitter.hbc.core.Client;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import pl.ynleborg.Producer;

import javax.annotation.PostConstruct;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@AllArgsConstructor
public class KafkaProducer extends Producer {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducer.class.getName());

    private KafkaTemplate<String, String> kafkaTemplate;

    @PostConstruct
    public void run() throws InterruptedException {
        Client client = null;
        try {
            LOG.info("START");
            BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
            client = createTwitterClient(msgQueue);
            client.connect();
            while (!client.isDone()) {
                String msg = msgQueue.poll(5, TimeUnit.SECONDS);
                if (msg != null) {
                    LOG.debug(msg);
                    kafkaTemplate.send("tweets", UUID.randomUUID().toString(), msg);
                }
            }
        } finally {
            if (client != null) {
                client.stop();
            }
        }
        LOG.info("STOP");
    }
}
