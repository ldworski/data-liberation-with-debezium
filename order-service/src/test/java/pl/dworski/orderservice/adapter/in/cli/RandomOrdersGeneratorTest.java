package pl.dworski.orderservice.adapter.in.cli;

import net.datafaker.Faker;
import org.junit.jupiter.api.Test;
import pl.dworski.orderservice.application.port.in.CreateOrderUseCase.CreateOrderCommand;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class RandomOrdersGeneratorTest {

    private final RandomOrdersGenerator generator = new RandomOrdersGenerator(
            new Faker(new Random(42)),
            new OrderGeneratorProperties(Duration.ofMillis(1), 3, 5, 1, 300));

    @Test
    void generatesOrdersWithinConfiguredLimits() {
        //when
        List<CreateOrderCommand> commands = Stream.generate(generator::randomOrderCommand).limit(20).toList();

        //then
        assertThat(commands).allSatisfy(command -> {
            assertThat(command.customerId()).isNotNull();
            assertThat(command.status()).isNotNull();
            assertThat(command.items()).hasSizeBetween(1, 3).allSatisfy(item -> {
                assertThat(item.itemName()).isNotBlank();
                assertThat(item.quantity()).isBetween(1, 5);
                assertThat(item.unitPrice()).isBetween(BigDecimal.ONE, BigDecimal.valueOf(300)).hasScaleOf(2);
            });
        });
        assertThat(commands).extracting(command -> command.items().size()).contains(1, 2, 3);
    }
}
