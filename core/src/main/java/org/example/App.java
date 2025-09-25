import org.example.KafkaManager;
import org.slf4j.LoggerFactory;

void main() throws Exception {
    final var LOG = LoggerFactory.getLogger("App");
    LOG.info("Starting kafka...");
    KafkaManager.runKafka();
}