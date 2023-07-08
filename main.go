package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"

	"github.com/ThreeDotsLabs/go-event-driven/common/clients"
	"github.com/ThreeDotsLabs/go-event-driven/common/clients/receipts"
	"github.com/ThreeDotsLabs/go-event-driven/common/clients/spreadsheets"
	commonHTTP "github.com/ThreeDotsLabs/go-event-driven/common/http"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"

	"golang.org/x/sync/errgroup"
)

type Money struct {
	MoneyAmount   string `json:"amount,omitempty"`
	MoneyCurrency string `json:"currency,omitempty"`
}

type Ticket struct {
	Id            string `json:"ticket_id,omitempty"`
	Status        string `json:"status,omitempty"`
	CustomerEmail string `json:"customer_email,omitempty"`
	Price         Money  `json:"price,omitempty"`
}

type TicketsStatusRequest struct {
	Tickets []Ticket `json:"tickets,omitempty"`
}

type IssueReceiptRequest struct {
	TicketID string
	Price    Money
}

func main() {
	logger := watermill.NewStdLogger(false, false)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_ADDR"),
	})

	pub, err := redisstream.NewPublisher(redisstream.PublisherConfig{
		Client: rdb,
	}, logger)
	if err != nil {
		panic(err)
	}

	sub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client: rdb,
	}, logger)
	if err != nil {
		panic(err)
	}

	clients, err := clients.NewClients(os.Getenv("GATEWAY_ADDR"), nil)
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
			fmt.Println("ticket data", ticket)
			eventData, err := json.Marshal(ticket)
			if err != nil {
				panic(err)
			}

			payload := message.NewMessage(watermill.NewShortUUID(), []byte(eventData))

			pub.Publish("issue-receipt", payload)
			pub.Publish("append-to-tracker", payload)

		}

		return c.NoContent(http.StatusOK)
	})

	e.GET("/health", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	router.AddNoPublisherHandler(
		"handler_issue_receipt",
		"issue-receipt", sub,
		func(message *message.Message) error {
			var eventData Ticket
			json.Unmarshal(message.Payload, &eventData)

			issueReceiptPayload := IssueReceiptRequest{
				TicketID: eventData.Id,
				Price:    eventData.Price,
			}

			return receiptsClient.IssueReceipt(context.Background(), issueReceiptPayload)
		})

	router.AddNoPublisherHandler(
		"handler_append_to_tracker",
		"append-to-tracker", sub,
		func(message *message.Message) error {
			var payload Ticket
			json.Unmarshal(message.Payload, &payload)
			return spreadsheetsClient.AppendRow(
				context.Background(),
				"tickets-to-print",
				[]string{payload.Id, payload.CustomerEmail, payload.Price.MoneyAmount, payload.Price.MoneyCurrency},
			)
		})

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
