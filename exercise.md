# Error logging

In the next several exercises, we'll look at a few ways to handle errors in handlers.

Middleware functions are a great place to keep the error-handling logic. 
There are two ways you can capture errors in middleware.

The first one is to store the return values in variables and return them.

```go
func HandleErrors(next message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		msgs, err := next(msg)
		
		if err != nil {
			// Handle the error 
		}
		
		return msgs, err
	}
}
```

The second one is to use `defer` and named returns. This is a different flavor of the same thing.

```go
func HandleErrors(next message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) (msgs []*message.Message, err error) {
		defer func() {
			if err != nil { 
				// Handle the error
			}
		}()

		return next(msg)
	}
}
```

Note that regardless of when in the sequence the middleware is added,
the error handling will be done at the end, after the handler and all other middleware functions are executed.
Previously, we used middleware that executed before the handler.
This pattern is a way to run some code after it.

## Exercise

File: `project/main.go`

Extend the logging middleware to also log errors.

The log message should be:

```
Message handling error
```

It should include two log fields: `error` with the error and `message_uuid` with the message UUID.


<div class="alert alert-dismissible bg-light-primary d-flex flex-column flex-sm-row p-7 mb-10">
    <div class="d-flex flex-column">
        <h3 class="mb-5 text-dark">
			<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-lightbulb text-primary" viewBox="0 0 16 16">
			  <path d="M2 6a6 6 0 1 1 10.174 4.31c-.203.196-.359.4-.453.619l-.762 1.769A.5.5 0 0 1 10.5 13a.5.5 0 0 1 0 1 .5.5 0 0 1 0 1l-.224.447a1 1 0 0 1-.894.553H6.618a1 1 0 0 1-.894-.553L5.5 15a.5.5 0 0 1 0-1 .5.5 0 0 1 0-1 .5.5 0 0 1-.46-.302l-.761-1.77a1.964 1.964 0 0 0-.453-.618A5.984 5.984 0 0 1 2 6zm6-5a5 5 0 0 0-3.479 8.592c.263.254.514.564.676.941L5.83 12h4.342l.632-1.467c.162-.377.413-.687.676-.941A5 5 0 0 0 8 1z"/>
			</svg>
			Tip
		</h3>
        <span>

There are two ways you can add multiple keys in logrus:

```go
logger.WithField("key1", value1).WithField("key2", value2).Info("Log message")
```

```go
logger.WithFields(logrus.Fields{
	"key1": value1, 
	"key2": value2,
}).Info("Log message")
```

</span>
	</div>
	</div>
