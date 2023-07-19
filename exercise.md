# Malformed messages

As much as retrying is useful most of the time, it doesn't work in all cases.

One example is a *malformed message*. This is a message that cannot be processed not because of an error on the handler side
but because the handler doesn't understand it. This could be a broken JSON message or a message sent to the wrong topic.

In this scenario, you need to remove the message from the queue.

We will show a few more advanced tools for this in future modules.
For now, let's consider a simple approach.

Your handler can remove the message by returning `nil` early instead of an error.

If it's an invalid message schema, you can check the metadata for it:

```go
if msg.Metadata.Get("type") != "booking.created" {
	log.Error("Invalid message type", nil)
	return nil
}
```

Returning `nil` in case of any JSON unmarshalling error might not be a good idea,
since you may lose some messages this way and never know about it.

If there's a particular message that got published by mistake and can't be unmarshalled,
you can check its UUID.

```go
if msg.UUID == "5f810ce3-222b-4626-bc04-cbfb460c98c7" {
	return nil
}
```

It's a primitive way of doing this, but it works and might be good enough for your use case.
It helps if you have a healthy CI/CD pipeline and can easily deploy a new version of the service.
Sometimes that's a pragmatic choice if you were to spend too much time on this.

It may be that errors on the business domain level are not retryable.
If you know this beforehand, you can create a dedicated error type and middleware that handles it.

```go
type PermanentError interface {
	IsPermanent() bool
}

func SkipPermanentErrorsMiddleware(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		msgs, err := h(msg)

		var permErr PermanentError
		if errors.As(err, &permErr) && permErr.IsPermanent() {
			return nil, nil
		}

		return msgs, err
	}
}
```

You can then use it in your application logic.
For example, if the message misses a critical field, there's no point in retrying it.
It's a good idea to raise some kind of alert when this error occurs.

```go
type MissingInvoiceNumber struct {}

func (m MissingInvoiceNumber) Error() string {
	return "missing the invoice number - can't continue"
}

func (m MissingInvoiceNumber) IsPermanent() bool {
	return true
}
```

## Exercise

File: `project/main.go`

A malformed `TicketBookingConfirmed` event with ID `2beaf5bc-d5e4-4653-b075-2b36bbf28949` was published on the Pub/Sub.
Add logic to your handlers to ignore and acknowledge it.

Additionally, add a `type` metadata for both the `TicketBookingConfirmed` and `TicketBookingCanceled` events.
It should be equal to the event name.

Then, update all your handlers to skip events with no `type` metadata set.
