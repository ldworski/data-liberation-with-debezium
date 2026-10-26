package pl.dworski.orderassembler.topology;

import pl.dworski.orderassembler.debezium.OrderItemRow;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

record OrderItems(List<OrderItemRow> items) {

    OrderItems {
        items = List.copyOf(items);
    }

    static OrderItems empty() {
        return new OrderItems(List.of());
    }

    OrderItems with(OrderItemRow item) {
        return new OrderItems(Stream.concat(without(item).items.stream(), Stream.of(item))
                .sorted(Comparator.comparing(OrderItemRow::id))
                .toList());
    }

    OrderItems without(OrderItemRow item) {
        return new OrderItems(items.stream().filter(existing -> !existing.id().equals(item.id())).toList());
    }
}
