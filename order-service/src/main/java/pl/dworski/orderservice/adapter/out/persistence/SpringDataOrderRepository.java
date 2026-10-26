package pl.dworski.orderservice.adapter.out.persistence;

import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.stereotype.Component;
import pl.dworski.orderservice.application.port.out.SaveOrderPort;
import pl.dworski.orderservice.domain.model.Order;
import pl.dworski.orderservice.domain.model.OrderItem;

import java.util.stream.Collectors;

@Component
class SpringDataOrderRepository implements SaveOrderPort {

    private final JdbcAggregateOperations aggregateOperations;

    SpringDataOrderRepository(JdbcAggregateOperations aggregateOperations) {
        this.aggregateOperations = aggregateOperations;
    }

    @Override
    public void save(Order order) {
        aggregateOperations.insert(toEntity(order));
    }

    private OrderEntity toEntity(Order order) {
        return new OrderEntity(
                order.orderNumber(),
                order.customerId(),
                order.status().name(),
                order.createdAt(),
                order.items().stream().map(this::toEntity).collect(Collectors.toSet()));
    }

    private OrderItemEntity toEntity(OrderItem item) {
        return new OrderItemEntity(item.id(), item.itemName(), item.quantity(), item.unitPrice());
    }
}
