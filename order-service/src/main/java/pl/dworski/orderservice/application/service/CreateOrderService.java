package pl.dworski.orderservice.application.service;

import org.springframework.transaction.annotation.Transactional;
import pl.dworski.orderservice.application.port.in.CreateOrderUseCase;
import pl.dworski.orderservice.application.port.out.SaveOrderPort;
import pl.dworski.orderservice.domain.model.Order;
import pl.dworski.orderservice.domain.model.OrderItem;

import java.time.Clock;

public class CreateOrderService implements CreateOrderUseCase {

    private final SaveOrderPort saveOrderPort;
    private final Clock clock;

    public CreateOrderService(SaveOrderPort saveOrderPort, Clock clock) {
        this.saveOrderPort = saveOrderPort;
        this.clock = clock;
    }

    @Override
    @Transactional
    public Order create(CreateOrderCommand command) {
        var items = command.items().stream()
                .map(item -> OrderItem.create(item.itemName(), item.quantity(), item.unitPrice()))
                .toList();
        var order = Order.create(command.customerId(), command.status(), clock.instant(), items);
        saveOrderPort.save(order);
        return order;
    }
}
