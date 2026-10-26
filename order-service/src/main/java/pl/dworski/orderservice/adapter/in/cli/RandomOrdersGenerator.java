package pl.dworski.orderservice.adapter.in.cli;

import net.datafaker.Faker;
import pl.dworski.orderservice.application.port.in.CreateOrderUseCase.CreateOrderCommand;
import pl.dworski.orderservice.domain.model.OrderStatus;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

public class RandomOrdersGenerator {

    private final Faker faker;
    private final OrderGeneratorProperties properties;

    public RandomOrdersGenerator(Faker faker, OrderGeneratorProperties orderGeneratorProperties) {
        this.faker = faker;
        this.properties = orderGeneratorProperties;
    }

    public CreateOrderCommand randomOrderCommand() {
        int itemCount = faker.number().numberBetween(1, properties.maxItems() + 1);
        List<CreateOrderCommand.Item> items = IntStream.range(0, itemCount)
                .mapToObj(_ -> randomItem())
                .toList();
        return new CreateOrderCommand(UUID.randomUUID(), faker.options().option(OrderStatus.class), items);
    }

    private CreateOrderCommand.Item randomItem() {
        return new CreateOrderCommand.Item(
                faker.commerce().productName(),
                faker.number().numberBetween(1, properties.maxQuantity() + 1),
                BigDecimal.valueOf(faker.number().randomDouble(2, properties.minUnitPrice(), properties.maxUnitPrice()))
                        .setScale(2, RoundingMode.HALF_UP));
    }
}
