package pl.dworski.orderassembler.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record OrderEvent(UUID orderNumber, UUID customerId, String status, Instant createdAt, List<Item> items) {

    public OrderEvent {
        items = List.copyOf(items);
    }

    public record Item(UUID id, String itemName, int quantity, BigDecimal unitPrice) {
    }
}
