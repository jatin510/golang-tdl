# Project: Migrate to Events

In the current form, our webhook handler knows much of what happens outside it.
It publishes a message for each action that needs to happen (issuing a receipt or appending the ticket to the tracker).
This is not a big deal right now, but handlers like this tend to grow and become hard to change.

We can easily improve this by replacing messages with a proper event.

To recap, here are some things to keep in mind regarding events:

- They're facts: They describe something that already happened.
- They're immutable: Once published, they can't be changed.
- They should be expressed as verbs in past tense, like `UserSignedUp`, `OrderPlaced`, or `AlarmTriggered`.

## Exercise

File: `project/main.go`

Instead of two messages published on the `issue-receipt` and `append-ticket` topics,
make the handler publish a single `TicketBookingConfirmed` event on the `TicketBookingConfirmed` topic.

The event should have the following form:

```json
{
  "header": {
    "id": "...",
    "published_at": "..."
  },
  "ticket_id": "...",
  "customer_email": "...",
  "price": {
    "amount": 100,
    "currency": "EUR"
  }
}
```

Change the Router handlers to subscribe to the `TicketBookingConfirmed` topic.

Keep two subscribers, as you need two separate consumer groups.
Otherwise, only one handler would receive each event.
