package pl.ynleborg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class TwitterProducer {

    private static Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

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
        logger.info("START");
        logger.info("STOP");
    }

}