package com.itheima.consumer.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class DelayExchangeConfig {

    @Bean
    public DirectExchange delayExchange() {
        return ExchangeBuilder
                .directExchange("delay.direct")
                .durable(true)
                .delayed()
                .build();
    }

    @Bean
    public Queue delayQueue() {
        return new Queue("delay.queue", true);
    }

    @Bean
    public Binding delayBinding() {
        return BindingBuilder.bind(delayQueue()).to(delayExchange()).with("delay");
    }
}
