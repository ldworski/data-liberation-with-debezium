package pl.dworski.orderservice.application.port.in;

import pl.dworski.orderservice.domain.model.Order;
import pl.dworski.orderservice.domain.model.OrderStatus;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

public interface CreateOrderUseCase {

    Order create(CreateOrderCommand command);

    record CreateOrderCommand(UUID customerId, OrderStatus status, List<Item> items) {

        public CreateOrderCommand {
            items = List.copyOf(items);
        }

        public record Item(String itemName, int quantity, BigDecimal unitPrice) {
        }
    }
}
