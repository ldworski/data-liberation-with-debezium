package pl.dworski.orderservice.domain.model;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public record Order(UUID orderNumber, UUID customerId, OrderStatus status, Instant createdAt,
                    List<OrderItem> items) {


    public Order {
        Objects.requireNonNull(orderNumber, "orderNumber must not be null");
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        Objects.requireNonNull(items, "items must not be null");
        Objects.requireNonNull(customerId, "customerId must not be null");
        if (items.isEmpty()) {
            throw new IllegalArgumentException("order must contain items");
        }
        items = List.copyOf(items);
    }

    public static Order create(UUID customerId, OrderStatus status, Instant createdAt, List<OrderItem> items) {
        return new Order(UUID.randomUUID(), customerId, status, createdAt, items);
    }
}
