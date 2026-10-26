package pl.dworski.orderassembler.config;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import pl.dworski.orderassembler.event.OrderEvent;
import pl.dworski.orderassembler.topology.OrderEventsTopology;
import tools.jackson.databind.json.JsonMapper;

import java.util.UUID;

@Configuration
@EnableKafkaStreams
@EnableConfigurationProperties(OrderAssemblerProperties.class)
class OrderAssemblerConfiguration {

    @Bean
    KStream<UUID, OrderEvent> orderEvents(StreamsBuilder streamsBuilder, OrderAssemblerProperties properties,
                                          JsonMapper jsonMapper) {
        return new OrderEventsTopology(properties.topics(), jsonMapper).buildOn(streamsBuilder);
    }
}
