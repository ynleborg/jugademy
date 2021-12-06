package pl.ynleborg.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import pl.ynleborg.Producer;

import javax.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Service
@Slf4j
@AllArgsConstructor
public class KafkaProducer extends Producer {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducer.class.getName());

    private KafkaTemplate<String, String> kafkaTemplate;

    private ObjectMapper jacksonObjectMapper;

    @PostConstruct
    public void run() throws JsonProcessingException, InterruptedException {

        LOG.info("START");
        int i = 0;
        while (i < 10000) {
            i++;
            String traceId = UUID.randomUUID().toString();
            String transactionId = "TXN" + String.format("%05d", i);
            Transaction t = Transaction.builder()
                    .amount(ThreadLocalRandom.current().nextLong(100))
                    .incomingDate(new Date()).transactionId(transactionId)
                    .build();
            kafkaTemplate.send(getStringStringProducerRecord(traceId, t, "events-in", t.getTransactionId()));
            // retry
            if (i % 10 == 0) {
                Thread.sleep(10);
                kafkaTemplate.send(getStringStringProducerRecord(traceId, t, "events-retry", t.getTransactionId()));
            }
            if (i % 100 == 0) {
                //dlq
                Thread.sleep(200);
                kafkaTemplate.send(getStringStringProducerRecord(traceId, t, "events-dlq", "TIMEOUT"));
            } else {
                //out
                t.setSettledDate(new Date());
                kafkaTemplate.send(getStringStringProducerRecord(traceId, t, "events-out", t.getTransactionId()));
            }

        }
        Thread.sleep(1000);
        LOG.info("STOP");
    }

    private ProducerRecord<String, String> getStringStringProducerRecord(String traceId, Transaction t, String
            topic, String transactionId) throws JsonProcessingException {
        var record = new ProducerRecord<>(topic, transactionId, jacksonObjectMapper.writeValueAsString(t));
        record.headers().add("traceId", traceId.getBytes(StandardCharsets.UTF_8));
        return record;
    }
}
