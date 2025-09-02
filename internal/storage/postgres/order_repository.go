package postgres

import (
	"WBTestTask0/internal/domain/models"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"log/slog"
	"time"
)

const (
	DuplicateKeyError = "23505"
)

var (
	ErrOrderAlreadyExists = errors.New("order already exists")
)

type Storage struct {
	db    *pgxpool.Pool
	cache map[string]*models.Order
}

func New(connString string) (*Storage, error) {
	const op = "order_repository.New"
	log := slog.With("op", op)
	db, err := pgxpool.New(context.Background(), connString)
	if err != nil {
		log.Error("Failed to connect to database", "err", err, "connString", connString)
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	err = db.Ping(context.Background())
	if err != nil {
		log.Error("Failed to ping database", "err", err, "connString", connString)
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &Storage{db: db, cache: make(map[string]*models.Order)}, nil
}

func (s *Storage) loadAllOrdersToCache() {
	const op = "order_repository.loadAllOrdersToCache"
	log := slog.With("op", op)
	ctx := context.Background()
	query := `SELECT order_uid FROM orders`
	rows, err := s.db.Query(ctx, query)
	if err != nil {
		log.Error("Failed to load orders for cache", "error", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var orderUID string
		if err := rows.Scan(&orderUID); err != nil {
			log.Error("Failed to scan order_uid for cache", "error", err)
			continue
		}
		order, err := s.GetOrderByID(orderUID, ctx)
		if err != nil || order == nil {
			log.Error("Failed to load order for cache", "order_uid", orderUID, "error", err)
			continue
		}
		s.cache[orderUID] = order
	}
	log.Info("Order cache initialized", "count", len(s.cache))
}

func (s *Storage) AddOrder(ctx context.Context, order models.Order) error {
	const op = "order_repository.AddOrder"
	log := slog.With("op", op)

	tx, err := s.db.Begin(ctx)
	if err != nil {
		log.Error("Failed to begin transaction", "err", err)
		return fmt.Errorf("%s: %w", op, err)
	}
	defer tx.Rollback(ctx)

	dateCreated, err := time.Parse(time.RFC3339, order.DateCreated)
	if err != nil {
		log.Error("Failed to parse date_created", "err", err)
		return fmt.Errorf("%s: %w", op, err)
	}

	const queryOrder = `
		INSERT INTO orders (
			order_uid, track_number, locale, internal_signature, 
			customer_id, delivery_service, shardkey, sm_id, 
			date_created, oof_shard
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`
	_, err = tx.Exec(ctx, queryOrder,
		order.OrderUID, order.TrackNumber, order.Locale, order.InternalSignature,
		order.CustomerID, order.DeliveryService, order.ShardKey, order.SmID,
		dateCreated, order.OofShard,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == DuplicateKeyError {
			log.Error("Order already exists", "order_uid", order.OrderUID)
			return ErrOrderAlreadyExists
		}
		log.Error("Failed to insert order", "order_uid", order.OrderUID, "err", err)
		return fmt.Errorf("%s: %w", op, err)
	}

	const queryDelivery = `
		INSERT INTO delivery (
			order_uid, name, phone, zip, city, address, region, email
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	_, err = tx.Exec(ctx, queryDelivery,
		order.OrderUID, order.Delivery.Name, order.Delivery.Phone, order.Delivery.Zip,
		order.Delivery.City, order.Delivery.Address, order.Delivery.Region, order.Delivery.Email,
	)
	if err != nil {
		log.Error("Failed to insert delivery", "order_uid", order.OrderUID, "err", err)
		return fmt.Errorf("%s: %w", op, err)
	}

	const queryPayment = `
		INSERT INTO payment (
			order_uid, transaction, request_id, currency, provider,
			amount, payment_dt, bank, delivery_cost, goods_total, custom_fee
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`
	_, err = tx.Exec(ctx, queryPayment,
		order.OrderUID, order.Payment.Transaction, order.Payment.RequestID, order.Payment.Currency,
		order.Payment.Provider, order.Payment.Amount, order.Payment.PaymentDT, order.Payment.Bank,
		order.Payment.DeliveryCost, order.Payment.GoodsTotal, fmt.Sprintf("%d", order.Payment.CustomFee),
	)
	if err != nil {
		log.Error("Failed to insert payment", "order_uid", order.OrderUID, "err", err)
		return fmt.Errorf("%s: %w", op, err)
	}

	const queryItem = `
		INSERT INTO items (
			id, order_uid, chrt_id, track_number, price, rid,
			name, sale, size, total_price, nm_id, brand, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
	`
	for _, item := range order.Items {
		itemID := uuid.New()

		_, err = tx.Exec(ctx, queryItem,
			itemID, order.OrderUID, item.ChrtID, item.TrackNumber, item.Price, item.Rid,
			item.Name, item.Sale, item.Size, item.TotalPrice, item.NmID, item.Brand, item.Status,
		)
		if err != nil {
			log.Error("Failed to insert item", "order_uid", order.OrderUID, "err", err)
			return fmt.Errorf("%s: %w", op, err)
		}
	}

	if err = tx.Commit(ctx); err != nil {
		log.Error("Failed to commit transaction", "order_uid", order.OrderUID, "err", err)
		return fmt.Errorf("%s: %w", op, err)
	}

	log.Info("Order successfully added", "order_uid", order.OrderUID)
	return nil
}

func (s *Storage) GetOrderByID(orderUID string, ctx context.Context) (*models.Order, error) {
	const op = "order_repository.GetOrderByID"
	log := slog.With("op", op)
	if order, ok := s.cache[orderUID]; ok {
		log.Info("Order found in cache", "order_uid", orderUID)
		return order, nil
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		log.Error("Failed to begin transaction", "error", err)
		return nil, err
	}

	var order models.Order
	queryOrder := `SELECT order_uid, track_number, entry, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard FROM orders WHERE order_uid = $1`
	err = tx.QueryRow(ctx, queryOrder, orderUID).Scan(
		&order.OrderUID,
		&order.TrackNumber,
		&order.Entry,
		&order.Locale,
		&order.InternalSignature,
		&order.CustomerID,
		&order.DeliveryService,
		&order.ShardKey,
		&order.SmID,
		&order.DateCreated,
		&order.OofShard,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Warn("Order not found", "order_uid", orderUID)

			return nil, nil
		}
		log.Error("Failed to select order", "order_uid", orderUID, "error", err)
		return nil, err
	}

	queryDelivery := `SELECT name, phone, zip, city, address, region, email FROM delivery WHERE order_uid = $1`
	err = tx.QueryRow(ctx, queryDelivery, orderUID).Scan(
		&order.Delivery.Name,
		&order.Delivery.Phone,
		&order.Delivery.Zip,
		&order.Delivery.City,
		&order.Delivery.Address,
		&order.Delivery.Region,
		&order.Delivery.Email,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Warn("Delivery not found", "order_uid", orderUID)
			return nil, nil
		}
		log.Error("Failed to select delivery", "order_uid", orderUID, "error", err)
		return nil, err
	}

	queryPayment := `SELECT transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee FROM payment WHERE order_uid = $1`
	err = tx.QueryRow(ctx, queryPayment, orderUID).Scan(
		&order.Payment.Transaction,
		&order.Payment.RequestID,
		&order.Payment.Currency,
		&order.Payment.Provider,
		&order.Payment.Amount,
		&order.Payment.PaymentDT,
		&order.Payment.Bank,
		&order.Payment.DeliveryCost,
		&order.Payment.GoodsTotal,
		&order.Payment.CustomFee,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Warn("Payment not found", "order_uid", orderUID)

			return nil, nil
		}
		log.Error("Failed to select payment", "order_uid", orderUID, "error", err)
		return nil, err
	}

	queryItems := `SELECT chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status FROM items WHERE order_uid = $1`
	rows, err := tx.Query(ctx, queryItems, orderUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var item models.Item
		err := rows.Scan(
			&item.ChrtID,
			&item.TrackNumber,
			&item.Price,
			&item.Rid,
			&item.Name,
			&item.Sale,
			&item.Size,
			&item.TotalPrice,
			&item.NmID,
			&item.Brand,
			&item.Status,
		)
		if err != nil {
			log.Error("Failed to scan item", "order_uid", orderUID, "error", err)
			return nil, err
		}
		order.Items = append(order.Items, item)
	}
	if err = rows.Err(); err != nil {
		log.Error("Error iterating over items", "order_uid", orderUID, "error", err)
		return nil, err
	}

	log.Info("Order retrieved successfully", "order_uid", order.OrderUID)

	s.cache[orderUID] = &order

	return &order, nil
}
