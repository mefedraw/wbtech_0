package services

import (
	"WBTestTask0/internal/domain/models"
	"context"
	"encoding/json"
	"log/slog"
)

type OrderRepository interface {
	loadAllOrdersToCache()
	AddOrder(ctx context.Context, order models.Order) error
	GetOrderByID(orderUID string, ctx context.Context) (*models.Order, error)
}
type OrderService struct {
	r OrderRepository
}

func NewOrderService(repository OrderRepository) *OrderService {
	return &OrderService{
		r: repository,
	}
}

func (s *OrderService) GetOrderByID(id string, ctx context.Context) (*models.Order, error) {
	const op = "OrderService.GetOrderByID"
	log := slog.With("op", op)
	order, err := s.r.GetOrderByID(id, ctx)
	if err != nil {
		log.Error("Failed to get order", "order_uid", id, "error", err)
		return nil, err
	}
	if order == nil {
		log.Info("No order found for id", "id", id)
		return nil, nil
	}
	return order, nil
}

func (s *OrderService) AddOrder(order models.Order, ctx context.Context) error {
	const op = "OrderService.AddOrder"
	log := slog.With("op", op)
	if err := s.r.AddOrder(ctx, order); err != nil {
		log.Error("Failed to add order", "error", err)

		return err
	}

	return nil
}

func (s *OrderService) StoreOrders(msgChan <-chan []byte) error {
	const op = "OrderService.StoreOrders"
	log := slog.With("op", op)
	var order models.Order
	for msg := range msgChan {
		if err := json.Unmarshal(msg, &order); err != nil {
			log.Error("Failed to unmarshal order", "error", err)
			return err
		}

		err := s.AddOrder(order, context.Background())
		if err != nil {
			log.Error("Failed to store order", "error", err)
			return err
		}
	}

	return nil
}
