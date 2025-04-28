package com.lm.backend.listeners;


import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class StrConsumerListener {

    @KafkaListener(groupId = "group-1",
            topicPartitions = @TopicPartition(topic = "str-topic", partitions = {"0"}),
            containerFactory = "validMessageContainerFactory") //ID que identifica a 1 grupo de consumidores. strContainerFactory contiene la informacion de ConsumerFactory
    public void listener1(String message) {
//        log.info("Recibiendo un mensaje {}", message);
        log.info("LISTENER1 ::: Recibiendo un mensaje {}", message );
    }

    @KafkaListener(groupId = "group-1",
            topicPartitions = @TopicPartition(topic = "str-topic", partitions = {"1"}),
            containerFactory = "validMessageContainerFactory")
    public void listener2(String message) {
//        log.info("Recibiendo un mensaje {}", message);
        log.info("LISTENER2 ::: Recibiendo un mensaje {}", message );
    }

    //En este caso por defecto escucha todas las particiones del grupo 2
    @KafkaListener(groupId = "group-2", topics = "str-topic", containerFactory = "validMessageContainerFactory") //ID que identifica a 1 grupo de consumidores. strContainerFactory contiene la informacion de ConsumerFactory
    public void listener3(String message) {
        log.info("Recibiendo un mensaje {}", message);
        log.info("LISTENER3 ::: Recibiendo un mensaje {}", message );
    }

//    @KafkaListener(groupId = "group-2", topics = "str-topic", containerFactory = "strContainerFactory") //ID que identifica a 1 grupo de consumidores. strContainerFactory contiene la informacion de ConsumerFactory
//    public void listener4(String message) {
//        log.info("Recibiendo un mensaje {}", message);
//        log.info("LISTENER4 ::: Recibiendo un mensaje {}", message );
//    }

}
