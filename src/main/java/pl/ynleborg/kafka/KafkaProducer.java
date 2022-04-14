package pl.ynleborg.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import pl.ynleborg.Producer;

import javax.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

@Service
@Slf4j
@AllArgsConstructor
public class KafkaProducer extends Producer {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducer.class.getName());

    private KafkaTemplate<String, String> kafkaTemplate;

    private ObjectMapper jacksonObjectMapper;

    @PostConstruct
    public void run() throws JsonProcessingException, InterruptedException {
        Random random = new Random();
        LOG.info("START");
        int i = 1;
        while (i <= 100) {
            i++;
            String traceId = RandomStringUtils.random(8, true, true);
            String transactionId = "TXN" + String.format("%05d", i);
            Transaction t = Transaction.builder()
                    .amount(ThreadLocalRandom.current().nextLong(100))
                    .incomingDate(new Date()).transactionId(transactionId)
                    .build();
            kafkaTemplate.send(getStringStringProducerRecord(traceId, t, "events-in", t.getTransactionId()));
            // retry

            if (i % 2 == 0 || i % 5 == 0) {
                Thread.sleep((long) (10 * random.nextFloat()));
                kafkaTemplate.send(getStringStringProducerRecord(traceId, t, "events-retry", t.getTransactionId()));
            }
            if (i % 5 == 0) {
                //dlq
                Thread.sleep((long) (100 * random.nextFloat()));
                kafkaTemplate.send(getStringStringProducerRecordDLQ(traceId, t, "events-dlq", t.getTransactionId()));
            } else {
                //out
                Thread.sleep((long) (100 * random.nextFloat()));
                t.setSettledDate(new Date());
                kafkaTemplate.send(getStringStringProducerRecord(traceId, t, "events-out", t.getTransactionId()));
            }

        }
        LOG.info("STOP");
    }

    private ProducerRecord<String, String> getStringStringProducerRecord(String traceId, Transaction t, String
            topic, String transactionId) throws JsonProcessingException {
        var record = new ProducerRecord<>(topic, transactionId, jacksonObjectMapper.writeValueAsString(t));
        record.headers().add("traceId", traceId.getBytes(StandardCharsets.UTF_8));
        return record;
    }

    private ProducerRecord<String, String> getStringStringProducerRecordDLQ(String traceId, Transaction t, String
            topic, String transactionId) throws JsonProcessingException {
        var record = new ProducerRecord<>(topic, transactionId, jacksonObjectMapper.writeValueAsString(t));
        record.headers().add("traceId", traceId.getBytes(StandardCharsets.UTF_8));
        record.headers().add("reason", "TIMEOUT".getBytes(StandardCharsets.UTF_8));
        record.headers().add("stack", ExceptionUtils.getStackTrace(new TimeoutException()).getBytes(StandardCharsets.UTF_8));
        return record;
    }
}
