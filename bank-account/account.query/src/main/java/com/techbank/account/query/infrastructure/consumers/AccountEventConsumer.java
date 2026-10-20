package com.techbank.account.query.infrastructure.consumers;

import com.techbank.account.query.infrastructure.handlers.EventHandler;
import com.techbank.cqrs.core.events.BaseEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.Logger;

@Service
public class AccountEventConsumer implements EventConsumer {
    private final Logger logger = Logger.getLogger(AccountEventConsumer.class.getName());

    @Autowired
    private EventHandler eventHandler;

    // Single topic -> Kafka keeps the order of the events within a partition.
    @KafkaListener(topics = "${spring.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    @Override
    public void consume(@Payload BaseEvent event, Acknowledgment ack) {
        logger.info("Consumed " + event.getClass().getSimpleName() + " v" + event.getVersion() + " for account " + event.getId());
        try {
            // Dispatch to the EventHandler.on(...) overload matching the concrete event type
            Method on = EventHandler.class.getMethod("on", event.getClass());
            on.invoke(eventHandler, event);
        } catch (NoSuchMethodException e) {
            logger.log(Level.WARNING, "No event handler registered for " + event.getClass().getName());
        } catch (IllegalAccessException | InvocationTargetException e) {
            logger.log(Level.SEVERE, "Error while handling event " + event.getClass().getName(), e);
            return; // no ack: the same event is redelivered, order is preserved
        }
        ack.acknowledge();
    }
}
