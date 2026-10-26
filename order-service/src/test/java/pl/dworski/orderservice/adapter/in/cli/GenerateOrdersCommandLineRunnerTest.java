package pl.dworski.orderservice.adapter.in.cli;

import net.datafaker.Faker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.DefaultApplicationArguments;
import pl.dworski.orderservice.application.port.in.CreateOrderUseCase;
import pl.dworski.orderservice.application.port.in.CreateOrderUseCase.CreateOrderCommand;
import pl.dworski.orderservice.domain.model.Order;
import pl.dworski.orderservice.domain.model.OrderItem;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class GenerateOrdersCommandLineRunnerTest {

    private static final Duration INTERVAL = Duration.ofMillis(1);
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    @Mock
    private CreateOrderUseCase createOrderUseCase;

    @Captor
    private ArgumentCaptor<CreateOrderCommand> createOrderCommandArgumentCaptor;

    private final RandomOrdersGenerator randomOrdersGenerator = new RandomOrdersGenerator(
            new Faker(new Random(42)),
            new OrderGeneratorProperties(INTERVAL, 3, 5, 1, 300));

    private Thread runnerThread;

    @AfterEach
    void stopRunner() throws InterruptedException {
        if (runnerThread != null && runnerThread.isAlive()) {
            runnerThread.interrupt();
            runnerThread.join(TIMEOUT);
        }
    }

    @Test
    void createsGeneratedOrdersUntilEnterIsPressed() throws IOException {
        //given
        when(createOrderUseCase.create(any())).thenAnswer(invocation -> toOrder(invocation.getArgument(0)));
        var stdin = new PipedOutputStream();
        startRunner(new PipedInputStream(stdin));
        await().atMost(TIMEOUT).untilAsserted(() -> verify(createOrderUseCase, atLeast(3)).create(any()));

        //when
        stdin.write("\n".getBytes(StandardCharsets.UTF_8));
        stdin.flush();

        //then
        await().atMost(TIMEOUT).until(() -> !runnerThread.isAlive());
        verify(createOrderUseCase, atLeast(3)).create(createOrderCommandArgumentCaptor.capture());
        assertThat(createOrderCommandArgumentCaptor.getAllValues()).allSatisfy(command -> {
            assertThat(command.customerId()).isNotNull();
            assertThat(command.status()).isNotNull();
            assertThat(command.items()).hasSizeBetween(1, 3);
        });
    }

    private void startRunner(InputStream inputStream) {
        var runner = new GenerateOrdersCommandLineRunner(createOrderUseCase, randomOrdersGenerator, INTERVAL, inputStream);
        runnerThread = Thread.ofPlatform().name("order-generator-under-test").start(() -> {
            try {
                runner.run(new DefaultApplicationArguments());
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            }
        });
    }

    private static Order toOrder(CreateOrderCommand command) {
        return Order.create(command.customerId(), command.status(), Instant.now(),
                command.items().stream()
                        .map(item -> OrderItem.create(item.itemName(), item.quantity(), item.unitPrice()))
                        .toList());
    }
}
