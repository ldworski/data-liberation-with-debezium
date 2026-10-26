package pl.dworski.orderassembler.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import pl.dworski.orderassembler.config.OrderAssemblerProperties.Topics;
import pl.dworski.orderassembler.event.OrderEvent;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class OrderEventsTopologyTest {

    private static final Topics TOPICS = new Topics("app.public.orders", "app.public.order_items", "order_events");
    private static final UUID ORDER_NUMBER = UUID.fromString("50035603-ad04-466f-9665-aa77e092a300");
    private static final UUID CUSTOMER_ID = UUID.fromString("08274926-7e8c-4d91-9ecb-ec11f3565e9d");
    private static final UUID KEYBOARD_ID = UUID.fromString("00000000-0000-0000-0000-000000000001");
    private static final UUID CABLE_ID = UUID.fromString("00000000-0000-0000-0000-000000000002");

    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> orders;
    private TestInputTopic<String, String> orderItems;
    private TestOutputTopic<UUID, OrderEvent> orderEvents;

    @BeforeEach
    void setUp() {
        var properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-assembler-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        properties.put(StreamsConfig.ENSURE_EXPLICIT_INTERNAL_RESOURCE_NAMING_CONFIG, true);
        var builder = new StreamsBuilder(new TopologyConfig(new StreamsConfig(properties)));
        new OrderEventsTopology(TOPICS, jsonMapper).buildOn(builder);
        driver = new TopologyTestDriver(builder.build(), properties);
        orders = driver.createInputTopic(TOPICS.orders(), new StringSerializer(), new StringSerializer());
        orderItems = driver.createInputTopic(TOPICS.orderItems(), new StringSerializer(), new StringSerializer());
        orderEvents = driver.createOutputTopic(TOPICS.orderEvents(),
                Serdes.UUID().deserializer(), new JacksonJsonDeserializer<>(OrderEvent.class, jsonMapper, false));
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void joinsOrderWithItsItemsIntoOrderEvent() {
        //given
        orders.pipeInput(orderKey(), orderChange("c", "PAID"));
        orderItems.pipeInput(itemKey(KEYBOARD_ID), itemChange("c", KEYBOARD_ID, "Mechanical Keyboard", 1, "89.90"));

        //when
        orderItems.pipeInput(itemKey(CABLE_ID), itemChange("c", CABLE_ID, "USB-C Cable", 2, "9.99"));

        //then
        assertThat(orderEvents.readKeyValuesToMap()).containsExactlyEntriesOf(Map.of(ORDER_NUMBER,
                new OrderEvent(ORDER_NUMBER, CUSTOMER_ID, "PAID", Instant.parse("2026-09-02T11:42:00Z"),
                        List.of(
                                new OrderEvent.Item(KEYBOARD_ID, "Mechanical Keyboard", 1, new BigDecimal("89.90")),
                                new OrderEvent.Item(CABLE_ID, "USB-C Cable", 2, new BigDecimal("9.99"))))));
    }

    @Test
    void publishesUpdatedOrderEventWhenOrderStatusChanges() {
        //given
        orders.pipeInput(orderKey(), orderChange("c", "PAID"));
        orderItems.pipeInput(itemKey(KEYBOARD_ID), itemChange("c", KEYBOARD_ID, "Mechanical Keyboard", 1, "89.90"));

        //when
        orders.pipeInput(orderKey(), orderChange("u", "SHIPPED"));

        //then
        assertThat(orderEvents.readKeyValuesToMap().get(ORDER_NUMBER).status()).isEqualTo("SHIPPED");
    }

    @Test
    void removesDeletedItemFromOrderEvent() {
        //given
        orders.pipeInput(orderKey(), orderChange("c", "PAID"));
        orderItems.pipeInput(itemKey(KEYBOARD_ID), itemChange("c", KEYBOARD_ID, "Mechanical Keyboard", 1, "89.90"));
        orderItems.pipeInput(itemKey(CABLE_ID), itemChange("c", CABLE_ID, "USB-C Cable", 2, "9.99"));

        //when
        orderItems.pipeInput(itemKey(CABLE_ID), deleteChange());
        orderItems.pipeInput(itemKey(CABLE_ID), (String) null);

        //then
        assertThat(orderEvents.readKeyValuesToMap().get(ORDER_NUMBER).items())
                .extracting(OrderEvent.Item::id)
                .containsExactly(KEYBOARD_ID);
    }

    @Test
    void publishesTombstoneWhenOrderIsDeleted() {
        //given
        orders.pipeInput(orderKey(), orderChange("c", "PAID"));
        orderItems.pipeInput(itemKey(KEYBOARD_ID), itemChange("c", KEYBOARD_ID, "Mechanical Keyboard", 1, "89.90"));

        //when
        orders.pipeInput(orderKey(), deleteChange());
        orders.pipeInput(orderKey(), (String) null);

        //then
        assertThat(orderEvents.readKeyValuesToMap()).containsEntry(ORDER_NUMBER, null);
    }

    private static String orderKey() {
        return """
                {"order_number":"%s"}""".formatted(ORDER_NUMBER);
    }

    private static String itemKey(UUID id) {
        return """
                {"id":"%s"}""".formatted(id);
    }

    private static String orderChange(String op, String status) {
        return """
                {"before":null,
                 "after":{"order_number":"%s","customer_id":"%s","status":"%s",
                          "created_at":"2026-09-02T11:42:00.000000Z"},
                 "source":{"connector":"postgresql","table":"orders"},
                 "op":"%s","ts_ms":1793612520000}""".formatted(ORDER_NUMBER, CUSTOMER_ID, status, op);
    }

    private static String itemChange(String op, UUID id, String itemName, int quantity, String unitPrice) {
        return """
                {"before":null,
                 "after":{"id":"%s","order_number":"%s","item_name":"%s","quantity":%d,"unit_price":"%s"},
                 "source":{"connector":"postgresql","table":"order_items"},
                 "op":"%s","ts_ms":1793612520000}""".formatted(id, ORDER_NUMBER, itemName, quantity, unitPrice, op);
    }

    private static String deleteChange() {
        return """
                {"before":null,"after":null,"source":{"connector":"postgresql"},"op":"d","ts_ms":1793612520000}""";
    }
}
