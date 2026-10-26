package pl.dworski.orderservice.application.port.out;

import pl.dworski.orderservice.domain.model.Order;

public interface SaveOrderPort {

    void save(Order order);
}
