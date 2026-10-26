package pl.dworski.orderservice.domain.model;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderItemTest {

    @Test
    void rejectsMissingItemName() {
        //when & then
        assertThatThrownBy(() -> OrderItem.create(null, 1, new BigDecimal("45.00")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("itemName must not be null");
    }

    @Test
    void rejectsBlankItemName() {
        //when & then
        assertThatThrownBy(() -> OrderItem.create(" ", 1, new BigDecimal("45.00")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("itemName must not be blank");
    }
}
