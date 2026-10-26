package pl.dworski.orderservice.domain.model;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.UUID;

public record OrderItem(UUID id, String itemName, int quantity, BigDecimal unitPrice) {

    public OrderItem {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(unitPrice, "unitPrice must not be null");
        Objects.requireNonNull(itemName, "itemName must not be null");
        if (itemName.isBlank()) {
            throw new IllegalArgumentException("itemName must not be blank");
        }
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be positive");
        }
        if (unitPrice.signum() < 0) {
            throw new IllegalArgumentException("unitPrice must not be negative");
        }
    }

    public static OrderItem create(String itemName, int quantity, BigDecimal unitPrice) {
        return new OrderItem(UUID.randomUUID(), itemName, quantity, unitPrice);
    }
}
