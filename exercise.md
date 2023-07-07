# Project: Health checks

You can now control how a service shuts down. Another good practice is being able to tell when it's up and ready to serve requests.

The simple, common way to do this is to expose an HTTP endpoint like `/health` that returns a 200 status code when the service is ready.

Here's an example using Echo:

```go
e.GET("/health", func(c echo.Context) error {
	return c.String(http.StatusOK, "ok")
})
```

The Router exposes a `Running` method that returns a channel that gets closed once the Router is ready.
You can use it like this:

```go
<-router.Running()
```

## Exercise

File: `project/main.go`

Implement a health check endpoint in your project.

The service should expose an HTTP `GET /health` endpoint that returns a 200 status code and an "ok" message.

Extend the code running your HTTP server so it waits for the Router to be ready.

```go
g.Go(func() error {
	<-router.Running()
	
	err := e.Start(":8080")
	if err != nil && err != http.ErrServerClosed {
		return err
	}
	
	return nil
})
```
