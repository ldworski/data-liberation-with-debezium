package pl.dworski.orderservice.domain.model;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderTest {

    private static final List<OrderItem> ITEMS = List.of(OrderItem.create("Desk Lamp", 1, new BigDecimal("45.00")));

    @Test
    void rejectsMissingCustomerId() {
        //when & then
        assertThatThrownBy(() -> Order.create(null, OrderStatus.NEW, Instant.now(), ITEMS))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("customerId must not be null");
    }
}
