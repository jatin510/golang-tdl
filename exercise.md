# Project: Graceful Shutdown

It's important to handle shutdowns gracefully in any production-grade application.
This means that the process should wait for all requests to finish before exiting.

Your handlers should be ready for the service getting suddenly killed anyway: Events like hardware failures or power outages can happen at any time.
One way to mitigate losing data this way is using database transactions. 
However, even so, it's a good idea to let  running requests finish. This way, your users won't notice
server restarts, and deployments become safer overall.

Shutting down the Router is easy: All you need to do is pass a context to the `Run` method.
Once the context is canceled, the Router stops accepting new requests and waits for the
running ones to finish.

To detect the application receiving an interrupt signal, use `signal.NotifyContext`, like this:

```go
ctx := context.Background()
ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
defer cancel()

err := router.Run(ctx)
if err != nil {
	panic(err)
}

<-ctx.Done()
```

Once the interrupt signal is received, the `ctx` will be canceled, and the Router will stop accepting new messages.
It will wait for the existing handlers to finish and then return without errors.

Your application would rarely be just the Router; there are often other long-running goroutines ("daemons") that need to be shut down gracefully as well.
To do that, you can use the `errgroup` package (`golang.org/x/sync/errgroup`), which allows you to run multiple goroutines and wait for them to finish.
(The `golang.org/x/` packages don't have the same API stability guarantee as the standard library, but it's good enough for our use case.) 

```go
ctx := context.Background()
ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
defer cancel()

g, ctx := errgroup.WithContext(ctx)

g.Go(func() error {
	return router.Run(ctx)
})

g.Go(func() error {
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
err := g.Wait()
if err != nil {
    panic(err)
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

If you need a refresher on how the `Context` works, the `ctx.Done()` channel is closed when the context is canceled.
So waiting for the context to be canceled is as simple as:

```go
<-ctx.Done()
```

</span>
	</div>
	</div>

To summarize how this works:

1. Create a new context, and pass it to `signal.NotifyContext`. The incoming interrupt signal will cancel the context.
2. Create a new `errgroup` and pass the context to it.
3. Start the Router in a new goroutine. It will stop accepting new requests once the context is canceled.
4. Start the HTTP server in a new goroutine.
5. Start a goroutine that will shut down the HTTP server once the context is canceled.
6. Wait for all goroutines to finish.

## Exercise

File: `project/main.go`

Introduce graceful Router shutdown in your project using `errgroup` and `signal.NotifyContext`.

Remember to add `golang.org/x/sync/errgroup` to your dependencies.
