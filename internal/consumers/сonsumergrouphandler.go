package consumers

import (
	"WBTestTask0/internal/domain/models"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"log/slog"
)

type ConsumerGroupHandler struct {
	msgChan chan<- []byte
}

func (h *ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	slog.Info("Consumer group session setup")
	return nil
}

func (h *ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	slog.Info("Consumer group session cleanup")
	return nil
}

func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	const op = "ConsumerGroupHandler.ConsumeClaim"
	log := slog.With("op", op)
	for msg := range claim.Messages() {
		log.Info("Message received",
			"topic", msg.Topic,
			"partition", msg.Partition,
			"offset", msg.Offset,
			"key", string(msg.Key),
			"value", string(msg.Value),
		)

		var order models.Order
		if err := json.Unmarshal(msg.Value, &order); err != nil {
			log.Error("Unmarshalling message failed", "error", err)
			continue
		}
		if err := validateOrder(&order); err != nil {
			log.Error("Order validation failed", "error", err, "order_uid", order.OrderUID)
			continue
		}

		h.msgChan <- msg.Value

		session.MarkMessage(msg, "")
	}
	return nil
}

func validateOrder(o *models.Order) error {
	if err := validateOrderFields(o); err != nil {
		return err
	}
	if err := validateDeliveryFields(&o.Delivery); err != nil {
		return err
	}
	if err := validatePaymentFields(&o.Payment); err != nil {
		return err
	}
	if err := validateItems(o.Items); err != nil {
		return err
	}
	return nil
}

func validateOrderFields(o *models.Order) error {
	if o.OrderUID == "" || o.TrackNumber == "" || o.Entry == "" ||
		o.Locale == "" || o.CustomerID == "" || o.DeliveryService == "" ||
		o.ShardKey == "" || o.SmID == 0 || o.DateCreated == "" || o.OofShard == "" {
		return errors.New("missing required order fields")
	}
	return nil
}

func validateDeliveryFields(d *models.Delivery) error {
	if d.Name == "" || d.Phone == "" || d.Zip == "" ||
		d.City == "" || d.Address == "" || d.Region == "" || d.Email == "" {
		return errors.New("missing required delivery fields")
	}
	return nil
}

func validatePaymentFields(p *models.Payment) error {
	if p.Transaction == "" || p.Currency == "" || p.Provider == "" ||
		p.Amount == 0 || p.PaymentDT == 0 || p.Bank == "" {
		return errors.New("missing required payment fields")
	}
	return nil
}

func validateItems(items []models.Item) error {
	if len(items) == 0 {
		return errors.New("items is empty")
	}
	for i, item := range items {
		if item.ChrtID == 0 || item.TrackNumber == "" || item.Price == 0 ||
			item.Rid == "" || item.Name == "" || item.Size == "" ||
			item.TotalPrice == 0 || item.NmID == 0 || item.Brand == "" || item.Status == 0 {
			return fmt.Errorf("missing required item fields in item %d", i)
		}
	}
	return nil
}
