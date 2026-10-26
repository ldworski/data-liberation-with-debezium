package pl.dworski.orderservice.adapter.out.persistence;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.MappedCollection;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

@Table("orders")
record OrderEntity(
        @Id UUID orderNumber,
        UUID customerId,
        String status,
        Instant createdAt,
        @MappedCollection(idColumn = "order_number") Set<OrderItemEntity> items) {
}
