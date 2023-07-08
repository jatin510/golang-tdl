package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/ThreeDotsLabs/go-event-driven/common/clients"
	"github.com/ThreeDotsLabs/go-event-driven/common/clients/receipts"
	"github.com/ThreeDotsLabs/go-event-driven/common/clients/spreadsheets"
	commonHTTP "github.com/ThreeDotsLabs/go-event-driven/common/http"
	"github.com/ThreeDotsLabs/go-event-driven/common/log"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/labstack/echo/v4"
	"github.com/lithammer/shortuuid/v3"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"

	"golang.org/x/sync/errgroup"
)

type Money struct {
	MoneyAmount   string `json:"amount,omitempty"`
	MoneyCurrency string `json:"currency,omitempty"`
}

type Ticket struct {
	Header        EventHeader `json:"header,omitempty"`
	Id            string      `json:"ticket_id,omitempty"`
	Status        string      `json:"status,omitempty"`
	CustomerEmail string      `json:"customer_email,omitempty"`
	Price         Money       `json:"price,omitempty"`
}

type TicketsStatusRequest struct {
	Tickets []Ticket `json:"tickets,omitempty"`
}

type IssueReceiptRequest struct {
	TicketID string
	Price    Money
}
type EventHeader struct {
	Id          string    `json:"id,omitempty"`
	PublishedAt time.Time `json:"published_at,omitempty"`
}

func NewEventHeader() EventHeader {
	return EventHeader{
		Id:          watermill.NewShortUUID(),
		PublishedAt: time.Now().UTC(),
	}
}

func LoggingMiddleware(next message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		logger := log.FromContext(msg.Context())
		logger = logger.WithField("message_uuid", watermill.NewUUID())

		logger.Info("Handling a message")

		return next(msg)
	}
}

func SetCorrelationIdMiddleware(next message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		ctx := msg.Context()

		correlationID := msg.Metadata.Get("correlation_id")
		if correlationID == "" {
			correlationID = shortuuid.New()
		}
		ctx = log.ToContext(ctx, logrus.WithFields(logrus.Fields{"correlation_id": correlationID}))
		ctx = log.ContextWithCorrelationID(ctx, correlationID)
		msg.SetContext(ctx)
		return next(msg)
	}
}

func ErrorHandlerMiddleware(next message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		msgs, err := next(msg)

		if err != nil {
			// ctx := msg.Context()

			logger := logrus.WithFields(logrus.Fields{
				"message_uuid": watermill.NewUUID(),
				"error":        err,
			})

			logger.Info("Message handling error")

		}

		return msgs, err
	}
}

func main() {
	log.Init(logrus.InfoLevel)
	logger := log.NewWatermill(logrus.NewEntry(logrus.StandardLogger()))

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	router.AddMiddleware(SetCorrelationIdMiddleware)
	router.AddMiddleware(LoggingMiddleware)

	rdb := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_ADDR"),
	})

	pub, err := redisstream.NewPublisher(redisstream.PublisherConfig{
		Client: rdb,
	}, logger)
	if err != nil {
		panic(err)
	}

	issueReceiptSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client:        rdb,
		ConsumerGroup: "issue-receipt",
	}, logger)
	if err != nil {
		panic(err)
	}

	appendToTrackerSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client:        rdb,
		ConsumerGroup: "append-to-tracker",
	}, logger)
	if err != nil {
		panic(err)
	}

	ticketsToRefundSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client: rdb,
		// ConsumerGroup: "append-to-tracker",
	}, logger)
	if err != nil {
		panic(err)
	}

	clients, err := clients.NewClients(
		os.Getenv("GATEWAY_ADDR"),
		func(ctx context.Context, req *http.Request) error {
			req.Header.Set("Correlation-ID", log.CorrelationIDFromContext(ctx))
			return nil
		})
	if err != nil {
		panic(err)
	}

	receiptsClient := NewReceiptsClient(clients)
	spreadsheetsClient := NewSpreadsheetsClient(clients)

	e := commonHTTP.NewEcho()

	e.POST("/tickets-status", func(c echo.Context) error {
		var request TicketsStatusRequest
		err := c.Bind(&request)
		if err != nil {
			return err
		}

		for _, ticket := range request.Tickets {
			ticket.Header = NewEventHeader()

			eventData, err := json.Marshal(ticket)
			if err != nil {
				panic(err)
			}

			if ticket.Status == "confirmed" {
				payload := message.NewMessage(watermill.NewShortUUID(), []byte(eventData))
				payload.Metadata.Set("correlation_id", c.Request().Header.Get("Correlation-ID"))

				err = pub.Publish("TicketBookingConfirmed", payload)
				if err != nil {
					panic(err)
				}
			} else if ticket.Status == "canceled" {
				payload := message.NewMessage(watermill.NewShortUUID(), []byte(eventData))
				payload.Metadata.Set("correlation_id", c.Request().Header.Get("Correlation-ID"))

				err = pub.Publish("TicketBookingCanceled", payload)
				if err != nil {
					panic(err)
				}
			}

		}

		return c.NoContent(http.StatusOK)
	})

	e.GET("/health", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	router.AddNoPublisherHandler(
		"handler_issue_receipt",
		"TicketBookingConfirmed",
		issueReceiptSub,
		func(message *message.Message) error {
			var eventData Ticket
			json.Unmarshal(message.Payload, &eventData)

			issueReceiptPayload := IssueReceiptRequest{
				TicketID: eventData.Id,
				Price:    eventData.Price,
			}

			return receiptsClient.IssueReceipt(message.Context(), issueReceiptPayload)
		})

	router.AddNoPublisherHandler(
		"handler_append_to_tracker",
		"TicketBookingConfirmed",
		appendToTrackerSub,
		func(message *message.Message) error {
			var payload Ticket
			json.Unmarshal(message.Payload, &payload)
			return spreadsheetsClient.AppendRow(
				message.Context(),
				"tickets-to-print",
				[]string{payload.Id, payload.CustomerEmail, payload.Price.MoneyAmount, payload.Price.MoneyCurrency},
			)
		})

	router.AddNoPublisherHandler(
		"handler_tickets_to_refund",
		"TicketBookingCanceled",
		ticketsToRefundSub,
		func(message *message.Message) error {
			var payload Ticket
			json.Unmarshal(message.Payload, &payload)
			return spreadsheetsClient.AppendRow(
				message.Context(),
				"tickets-to-refund",
				[]string{payload.Id, payload.CustomerEmail, payload.Price.MoneyAmount, payload.Price.MoneyCurrency},
			)
		})

	router.AddMiddleware(ErrorHandlerMiddleware)
	logrus.Info("Server starting...")

	ctx := context.Background()

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return router.Run(ctx)
	})

	g.Go(func() error {
		<-router.Running()

		err := e.Start(":8080")
		if err != nil && err != http.ErrServerClosed {
			return err
		}

		return nil
	})

	g.Go(func() error {
		// Shut down the HTTP server
		<-ctx.Done()
		return e.Shutdown(ctx)
	})

	// Will block until all goroutines finish
	err = g.Wait()
	if err != nil {
		panic(err)
	}
}

type ReceiptsClient struct {
	clients *clients.Clients
}

func NewReceiptsClient(clients *clients.Clients) ReceiptsClient {
	return ReceiptsClient{
		clients: clients,
	}
}

func (c ReceiptsClient) IssueReceipt(ctx context.Context, request IssueReceiptRequest) error {
	body := receipts.PutReceiptsJSONRequestBody{
		TicketId: request.TicketID,
		Price:    receipts.Money(request.Price),
	}

	receiptsResp, err := c.clients.Receipts.PutReceiptsWithResponse(ctx, body)
	if err != nil {
		return err
	}
	if receiptsResp.StatusCode() != http.StatusOK {
		return fmt.Errorf("unexpected status code: %v", receiptsResp.StatusCode())
	}

	return nil
}

type SpreadsheetsClient struct {
	clients *clients.Clients
}

func NewSpreadsheetsClient(clients *clients.Clients) SpreadsheetsClient {
	return SpreadsheetsClient{
		clients: clients,
	}
}

func (c SpreadsheetsClient) AppendRow(ctx context.Context, spreadsheetName string, row []string) error {
	request := spreadsheets.PostSheetsSheetRowsJSONRequestBody{
		Columns: row,
	}

	sheetsResp, err := c.clients.Spreadsheets.PostSheetsSheetRowsWithResponse(ctx, spreadsheetName, request)
	if err != nil {
		return err
	}
	if sheetsResp.StatusCode() != http.StatusOK {
		return fmt.Errorf("unexpected status code: %v", sheetsResp.StatusCode())
	}

	return nil
}
