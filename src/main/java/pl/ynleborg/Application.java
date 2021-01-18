package pl.ynleborg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class Application {

    private static final Logger LOG = LoggerFactory.getLogger(Application.class.getName());

    public static void main(String[] args) throws InterruptedException {
        Properties prop = new Properties();
        try (InputStream input = TwitterProducer.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                LOG.error("Sorry, unable to find config.properties");
                return;
            }
            prop.load(input);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
        new TwitterProducer().run(prop);
    }
}
