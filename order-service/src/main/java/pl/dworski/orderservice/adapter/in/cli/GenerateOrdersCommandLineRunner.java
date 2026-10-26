package pl.dworski.orderservice.adapter.in.cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import pl.dworski.orderservice.application.port.in.CreateOrderUseCase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Duration;

public class GenerateOrdersCommandLineRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(GenerateOrdersCommandLineRunner.class);

    private final CreateOrderUseCase createOrderUseCase;
    private final RandomOrdersGenerator randomOrdersGenerator;
    private final Duration interval;
    private final InputStream inputStream;
    private volatile boolean running = true;

    public GenerateOrdersCommandLineRunner(CreateOrderUseCase createOrderUseCase,
                                           RandomOrdersGenerator randomOrdersGenerator,
                                           Duration interval,
                                           InputStream inputStream) {
        this.createOrderUseCase = createOrderUseCase;
        this.randomOrdersGenerator = randomOrdersGenerator;
        this.interval = interval;
        this.inputStream = inputStream;
    }

    @Override
    public void run(ApplicationArguments args) throws InterruptedException {
        listenForEnterToStopDataGeneration();
        log.info("Generating orders every {} - press ENTER to stop", interval);
        while (running) {
            var order = createOrderUseCase.create(randomOrdersGenerator.randomOrderCommand());
            log.info("Created order {} [{}] for {}: {} item(s)",
                    order.orderNumber(), order.status(), order.customerId(), order.items().size());
            Thread.sleep(interval);
        }
    }

    private void listenForEnterToStopDataGeneration() {
        Thread.ofPlatform().daemon().name("stdin-listener").start(() -> {
            try {
                if (new BufferedReader(new InputStreamReader(inputStream)).readLine() != null) {
                    log.info("ENTER pressed, stopping");
                    running = false;
                } else {
                    log.info("No terminal input available, stop with Ctrl+C");
                }
            } catch (IOException e) {
                log.warn("Cannot read from stdin, stop with Ctrl+C", e);
            }
        });
    }
}
