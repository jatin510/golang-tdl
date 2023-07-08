# Project: Handle Cancellations 


<div class="alert alert-dismissible bg-info text-white d-flex flex-column flex-sm-row p-7 mb-10">
    <div class="d-flex flex-column">
        <h3 class="mb-5 text-white">
			Background	
		</h3>
        <span>

With more tickets to handle, refunds also become more frequent.
Because our webhook is asynchronous, it's possible that a ticket gets canceled
right after the purchase because someone else bought it first.
Right now, our operations team handles these cases manually; it's time we help them out a bit.

</span>
	</div>
	</div>

## Exercise

File: `project/main.go`

The new API includes a `status` field for each ticket.

We should differentiate between `confirmed` and `canceled` tickets. 

For each `confirmed` ticket, keep the current behavior: publishing the `TicketBookingConfirmed` event.

For each `canceled` ticket, publish a new event instead: `TicketBookingCanceled`.

```go
type TicketBookingCanceled struct {
	Header        EventHeader `json:"header"`
	TicketID      string      `json:"ticket_id"`
	CustomerEmail string      `json:"customer_email"`
	Price         Money       `json:"price"`
}
```

Add a new handler for this event. Remember to use a new subscriber with a unique consumer group.

The new handler should append a row to the `tickets-to-refund` spreadsheet with the following columns:

- Ticket ID
- Customer Email
- Price Amount
- Price Currency
