package consumers

import (
	"context"
	"github.com/IBM/sarama"
	"log/slog"
)

const topic = "service.message"

type KafkaConsumer struct {
	Kafka sarama.ConsumerGroup
}

func New(brokers []string, groupID string) *KafkaConsumer {
	const op = "consumers.Consume"
	log := slog.With("op", op)
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V2_1_0_0

	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		log.Error("Couldn't create kafka consumer group", "error", err)
		return nil
	}

	log.Info("Kafka consumer group created successfully")

	return &KafkaConsumer{
		Kafka: consumerGroup,
	}
}

func (kc *KafkaConsumer) Consume(msgChan chan<- []byte) {
	const op = "consumers.Consume"
	log := slog.With("op", op)
	handler := &ConsumerGroupHandler{msgChan: msgChan}
	ctx := context.Background()
	go func() {
		for {
			if err := kc.Kafka.Consume(ctx, []string{topic}, handler); err != nil {
				log.Error("Error from consumer group", "error", err)
			}
		}
	}()
}
