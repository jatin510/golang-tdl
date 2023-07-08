# Project: Correlation ID

In bigger projects, it might make sense to use your own middleware to set the correlation ID.
It should check if the correlation ID is already present in the message's metadata and, if so,
add it to the message's context. Any further requests and messages can then use the same correlation ID.


<div class="alert alert-dismissible bg-light-primary d-flex flex-column flex-sm-row p-7 mb-10">
    <div class="d-flex flex-column">
        <h3 class="mb-5 text-dark">
			<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-lightbulb text-primary" viewBox="0 0 16 16">
			  <path d="M2 6a6 6 0 1 1 10.174 4.31c-.203.196-.359.4-.453.619l-.762 1.769A.5.5 0 0 1 10.5 13a.5.5 0 0 1 0 1 .5.5 0 0 1 0 1l-.224.447a1 1 0 0 1-.894.553H6.618a1 1 0 0 1-.894-.553L5.5 15a.5.5 0 0 1 0-1 .5.5 0 0 1 0-1 .5.5 0 0 1-.46-.302l-.761-1.77a1.964 1.964 0 0 0-.453-.618A5.984 5.984 0 0 1 2 6zm6-5a5 5 0 0 0-3.479 8.592c.263.254.514.564.676.941L5.83 12h4.342l.632-1.467c.162-.377.413-.687.676-.941A5 5 0 0 0 8 1z"/>
			</svg>
			Tip
		</h3>
        <span>

`context.Context` is often used to pass arbitrary data between functions.
You shouldn't overuse it, but it works well for passing data that's not directly related to the function's logic.

A good rule of thumb is that the function should work the same way when a `context.Background()` is passed to it.
Keep the values optional.

</span>
	</div>
	</div>

First, retrieve the correlation ID from the message's metadata:

```go
correlationID := msg.Metadata.Get("correlation_id")
```

If it's not present, it's a good idea to generate a new one.
Even if you don't see the full context this way, you can trace at least a part of the request.

```go
if correlationID == "" {
	correlationID = shortuuid.New()
}
```


<div class="alert alert-dismissible bg-light-primary d-flex flex-column flex-sm-row p-7 mb-10">
    <div class="d-flex flex-column">
        <h3 class="mb-5 text-dark">
			<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-lightbulb text-primary" viewBox="0 0 16 16">
			  <path d="M2 6a6 6 0 1 1 10.174 4.31c-.203.196-.359.4-.453.619l-.762 1.769A.5.5 0 0 1 10.5 13a.5.5 0 0 1 0 1 .5.5 0 0 1 0 1l-.224.447a1 1 0 0 1-.894.553H6.618a1 1 0 0 1-.894-.553L5.5 15a.5.5 0 0 1 0-1 .5.5 0 0 1 0-1 .5.5 0 0 1-.46-.302l-.761-1.77a1.964 1.964 0 0 0-.453-.618A5.984 5.984 0 0 1 2 6zm6-5a5 5 0 0 0-3.479 8.592c.263.254.514.564.676.941L5.83 12h4.342l.632-1.467c.162-.377.413-.687.676-.941A5 5 0 0 0 8 1z"/>
			</svg>
			Tip
		</h3>
        <span>

If you want to make it more obvious that the correlation ID was missing,
you can add a prefix to the newly generated one, like `gen_`.

</span>
	</div>
	</div>

To add the correlatioID to the context, you can use the `log.ContextWithCorrelationID` function from the common package.

```go
ctx := log.ContextWithCorrelationID(msg.Context(), reqCorrelationID)

msg.SetContext(ctx)
```

## Exercise

File: `project/main.go`

Add middleware to propagate the correlation ID from the metadata to context, as described above.

For all published messages, propagate the correlation ID from the HTTP `Correlation-ID` header
into the `correlation_id` metadata, like this:

```go
msg.Metadata.Set("correlation_id", c.Request().Header.Get("Correlation-ID"))
```

Finally, modify the `clients` constructor.
Add a *request editor* that propagates the correlation ID from the context to the HTTP request's header.

```go
clients, err := clients.NewClients(
	os.Getenv("GATEWAY_ADDR"), 
	func(ctx context.Context, req *http.Request) error {
		req.Header.Set("Correlation-ID", log.CorrelationIDFromContext(ctx))
		return nil
	},
)
```


<div class="alert alert-dismissible bg-light-primary d-flex flex-column flex-sm-row p-7 mb-10">
    <div class="d-flex flex-column">
        <h3 class="mb-5 text-dark">
			<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-lightbulb text-primary" viewBox="0 0 16 16">
			  <path d="M2 6a6 6 0 1 1 10.174 4.31c-.203.196-.359.4-.453.619l-.762 1.769A.5.5 0 0 1 10.5 13a.5.5 0 0 1 0 1 .5.5 0 0 1 0 1l-.224.447a1 1 0 0 1-.894.553H6.618a1 1 0 0 1-.894-.553L5.5 15a.5.5 0 0 1 0-1 .5.5 0 0 1 0-1 .5.5 0 0 1-.46-.302l-.761-1.77a1.964 1.964 0 0 0-.453-.618A5.984 5.984 0 0 1 2 6zm6-5a5 5 0 0 0-3.479 8.592c.263.254.514.564.676.941L5.83 12h4.342l.632-1.467c.162-.377.413-.687.676-.941A5 5 0 0 0 8 1z"/>
			</svg>
			Tip
		</h3>
        <span>

In your handlers, make sure you pass the `msg.Context()` to the external HTTP calls, not `context.Background()`.
Otherwise, the correlation ID won't be propagated.

</span>
	</div>
	</div>
