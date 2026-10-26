package pl.dworski.orderservice.adapter.out.persistence;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;
import java.util.UUID;

@Table("order_items")
record OrderItemEntity(
        @Id UUID id,
        String itemName,
        int quantity,
        BigDecimal unitPrice) {
}
