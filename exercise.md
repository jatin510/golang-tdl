# Temporary errors

While processing a message, a handler can return an error.
This is normal and expected. There are a few types of errors that can happen,
and to build a solid system, you need to handle them properly.

When a handler returns an error, Watermill's Router will send a "negative acknowledge" (*nack*)
to the Pub/Sub. Most of the time, this puts the message back on the queue to be delivered again.

Unless your handlers are trivial, they will need to reach out
over the network or to the file system, and these operations can fail.
These errors are usually temporary; for example, the database might be down for a while,
but when it comes back up, you can try again.

The preferred way of dealing with temporary errors is simply retrying.
The message is delivered again and again until it succeeds.

This approach seems trivial, but it's quite powerful. It allows you to not worry about
temporary issues. The system auto-heals without human intervention.

It's up to the Pub/Sub to decide how to handle the *nack*.
A common approach is to immediately redeliver the message.
The downside of this approach is that it can cause a big load spike to impact your service.
If the database is down for a few minutes, it makes no sense to retry every few milliseconds.

Some Pub/Subs can be configured to delay the redelivery, for example, by a few seconds. 
This is an improvement, as it spreads the load over time.
However, in this scenario, the Pub/Sub continues to deliver other messages.
It means the *nacked* message will be delivered out of order. This can be a problem or not,
depending on the use case and how your handlers work.

## Exercise

File: `project/main.go`

Watermill provides middleware that can be used to retry messages.
It reacts to errors, and before sending a *nack*, it tries to process the message again.

```go
middleware.Retry{
	MaxRetries:      10, 
	InitialInterval: time.Millisecond * 100, 
	MaxInterval:     time.Second, 
	Multiplier:      2, 
	Logger:          watermillLogger,
}
```

You can configure it for exponential backoff, so the delay between retries increases after each error.

Add a retry middleware function to your project, so the messages get processed even if the Receipts API is down for some time.

Use exponential backoff: If you spam the API, the service might have issues coming online.
