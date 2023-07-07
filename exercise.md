# Project: The Router

## Exercise

File: `project/main.go`

Rework your project to use the Router.

Remove the `Subscribe` calls and iteration over the messages.
Replace them with `AddNoPublisherHandler` methods.

You can name your handlers however you want, but keep the same topic names.
Keep two subscribers, each with its own consumer group; each handler should use one of the subscribers.

Remember: When using the Router, you don't need to call `Ack()` or `Nack()` explicitly.
It's enough to return the proper error from the handler function.

There's already a blocking HTTP server running in the `main` function.
To run the Router, you need to call `Run` in a separate goroutine.

```go
go func() {
	err := router.Run(context.Background())
	if err != nil {
		panic(err)
	}
}()
```
