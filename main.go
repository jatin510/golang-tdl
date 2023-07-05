package main

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/ThreeDotsLabs/go-event-driven/common/clients"
	"github.com/ThreeDotsLabs/go-event-driven/common/clients/receipts"
	"github.com/ThreeDotsLabs/go-event-driven/common/clients/spreadsheets"
	commonHTTP "github.com/ThreeDotsLabs/go-event-driven/common/http"
	"github.com/ThreeDotsLabs/go-event-driven/common/log"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type TicketsConfirmationRequest struct {
	Tickets []string `json:"tickets"`
}

func main() {
	log.Init(logrus.InfoLevel)

	clients, err := clients.NewClients(os.Getenv("GATEWAY_ADDR"), nil)
	if err != nil {
		panic(err)
	}

	// receiptsClient := NewReceiptsClient(clients)
	// spreadsheetsClient := NewSpreadsheetsClient(clients)

	e := commonHTTP.NewEcho()

	w := NewWorker()

	go w.Run(clients)

	e.POST("/tickets-confirmation", func(c echo.Context) error {
		var request TicketsConfirmationRequest
		err := c.Bind(&request)
		if err != nil {
			return err
		}

		for _, ticket := range request.Tickets {
			// err = receiptsClient.IssueReceipt(c.Request().Context(), ticket)
			// if err != nil {
			// 	return err
			// }

			// err = spreadsheetsClient.AppendRow(c.Request().Context(), "tickets-to-print", []string{ticket})
			// if err != nil {
			// 	return err
			// }
			w.Send(Message{
				Task:     TaskIssueReceipt,
				TicketId: ticket,
			}, Message{
				Task:     TaskAppendToTracker,
				TicketId: ticket,
			})

		}

		return c.NoContent(http.StatusOK)
	})

	logrus.Info("Server starting...")

	err = e.Start(":8080")
	if err != nil && err != http.ErrServerClosed {
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

func (c ReceiptsClient) IssueReceipt(ctx context.Context, ticketID string) error {
	body := receipts.PutReceiptsJSONRequestBody{
		TicketId: ticketID,
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

type Worker struct {
	queue chan Message
}

func NewWorker() *Worker {
	return &Worker{
		queue: make(chan Message, 100),
	}
}

type Task int

const (
	TaskIssueReceipt Task = iota
	TaskAppendToTracker
)

type Message struct {
	Task     Task
	TicketId string
}

func (w Worker) Send(msg ...Message) {
	publisher, _ := Publisher()

	for _, m := range msg {
		publishMessage := message.NewMessage(watermill.NewShortUUID(), []byte(m.TicketId))

		if m.Task == 0 {
			publisher.Publish("issue-receipt", publishMessage)
			fmt.Println("publish is successful: issue-receipt")
		} else if m.Task == 1 {
			publisher.Publish("append-to-tracker", publishMessage)
			fmt.Println("publish is successful: append-to-trackers")
		}

	}
}

func (w Worker) Run(clients *clients.Clients) {
	subscriber, _ := Subscriber()

	go SubscribeIssueReceipt(subscriber, clients)
	go SubscribeAppendToTracker(subscriber, clients)
}

func SubscribeIssueReceipt(subscriber *redisstream.Subscriber, clients *clients.Clients) {
	receiptsClient := NewReceiptsClient(clients)

	// subscriber for topic: issue-receipt
	messages, err := subscriber.Subscribe(context.Background(), "issue-receipt")
	if err != nil {
		panic(err)
	}

	fmt.Println("subscribe is successful: issue-receipt")

	for msg := range messages {
		ticketId := string(msg.Payload)
		err := receiptsClient.IssueReceipt(context.Background(), ticketId)
		if err != nil {
			msg.Nack()
		}
		msg.Ack()
	}
}

func SubscribeAppendToTracker(subscriber *redisstream.Subscriber, clients *clients.Clients) {
	spreadsheetsClient := NewSpreadsheetsClient(clients)

	// subscriber for topic: append-to-tracker
	messages, err := subscriber.Subscribe(context.Background(), "append-to-tracker")
	if err != nil {
		panic(err)
	}
	fmt.Println("subscribe is successful: append to tracker")

	for msg := range messages {
		ticketId := string(msg.Payload)
		err := spreadsheetsClient.AppendRow(context.Background(), "tickets-to-print", []string{ticketId})
		if err != nil {
			msg.Nack()
		}
		msg.Ack()
	}

}

func Publisher() (*redisstream.Publisher, error) {

	logger := watermill.NewStdLogger(false, false)

	rdb := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_ADDR"),
	})

	publisher, err := redisstream.NewPublisher(redisstream.PublisherConfig{
		Client: rdb,
	}, logger)
	if err != nil {
		panic(err)
	}

	return publisher, nil

}

func Subscriber() (*redisstream.Subscriber, error) {

	logger := watermill.NewStdLogger(false, false)

	rdb := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_ADDR"),
	})

	subscriber, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client: rdb,
	}, logger)
	if err != nil {
		panic(err)
	}

	return subscriber, nil

}
