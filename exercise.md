# Code Bugs

One more source of errors is the classic code bug. 

For example, suppose the event payload contains a phone number:

```go
type BookingCreated struct {
	BookingID string `json:"booking_id"`
	Phone     string `json:"phone"`
}
```

The publisher follows the contract and sends a payload:

```json
{
    "booking_id": "c7d6e6c0-19b9-4783-97d7-89eee336fa26",
    "phone": "+123456789"
}
```

However, your handler expects the phone number to be in a different format. You assumed the country code would be there,
but without the plus sign:

```go
phonePattern := regexp.MustCompile(`^\d+$`)

if !phonePattern.MatchString(bookingCreated.Phone) {
	return errors.New("invalid phone number")
}
```

The messages start queueing up and get redelivered, failing endlessly.

The solution is the *fix-forward* approach.
You have to fix the regexp:

```go
phonePattern := regexp.MustCompile(`^\+\d+$`)
```

Once you deploy the new version of the service, all messages will eventually be processed.

This is where the retry approach shines: You don't need to worry about the stuck messages.
Temporary errors should auto-heal, and you can easily fix code bugs without manual intervention.

## Exercise

File: `project/main.go`

For a brief time, we've been publishing incorrect `TicketBookingConfirmed` events.
They were missing the `Currency` field. Thankfully, this happened only for tickets bought with `USD`.

Update your handlers to use the `USD` currency if it's missing in the event.
