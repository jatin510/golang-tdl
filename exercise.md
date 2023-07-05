# Project: Introduce the Pub/Sub

It's time to introduce a real Pub/Sub to your project.

We'll start with Redis, since it's lightweight and easy to configure.


<div class="alert alert-dismissible bg-light-primary d-flex flex-column flex-sm-row p-7 mb-10">
    <div class="d-flex flex-column">
        <h3 class="mb-5 text-dark">
			<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-lightbulb text-primary" viewBox="0 0 16 16">
			  <path d="M2 6a6 6 0 1 1 10.174 4.31c-.203.196-.359.4-.453.619l-.762 1.769A.5.5 0 0 1 10.5 13a.5.5 0 0 1 0 1 .5.5 0 0 1 0 1l-.224.447a1 1 0 0 1-.894.553H6.618a1 1 0 0 1-.894-.553L5.5 15a.5.5 0 0 1 0-1 .5.5 0 0 1 0-1 .5.5 0 0 1-.46-.302l-.761-1.77a1.964 1.964 0 0 0-.453-.618A5.984 5.984 0 0 1 2 6zm6-5a5 5 0 0 0-3.479 8.592c.263.254.514.564.676.941L5.83 12h4.342l.632-1.467c.162-.377.413-.687.676-.941A5 5 0 0 0 8 1z"/>
			</svg>
			Tip
		</h3>
        <span>

Keep in mind that Redis might or might not be a good choice for your real-world project.
Consider the tradeoffs and your needs before you commit to one of the Pub/Subs.

</span>
	</div>
	</div>

### Running it locally

This exercise includes a `docker-compose.yml` file you can use to run the solution locally.

First, run `docker-compose up`. In another terminal, start your solution.
You will need to provide two environment variables:

```bash
REDIS_ADDR=localhost:6379 GATEWAY_ADDR=localhost:8888 go run .
```

The API should be up and running on `localhost:8080`.

## Exercise

File: `project/main.go`

Introduce the Redis Stream Pub/Sub to the project.
Replace the worker implementation with Watermill publishers and subscribers.

Here are some tips to get you started:

* Instead of sending worker messages, publish Watermill messages on two topics: `issue-receipt` and `append-to-tracker`.
* Make TicketID the payload (simply cast the string to `[]byte`).
* Create **two subscribers**, one for each topic. Each should use a unique consumer group.
* Iterate over incoming messages and execute the logic. Move the logic from the worker's `Run()` method.
* Watermill's `Message` exposes the context via the `Context()` method. Replace `context.Background()` with it.

Remember to run each iteration in a separate goroutine. Otherwise, you'll block the main function.
Don't forget about error handling!

```go
go func() {
	messages, err := sub.Subscribe(context.Background(), "topic")
	if err != nil {
		panic(err)
	}
	
	for msg := range messages {
		err := Action()
		if err != nil {
			msg.Nack()
		} else {
			msg.Ack()
		}
	}
}()
```

You may need to `go get` the dependencies:

```bash
go get github.com/ThreeDotsLabs/watermill
go get github.com/ThreeDotsLabs/watermill-redisstream
go get github.com/redis/go-redis/v9
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

You can create a Watermill logger out of the logrus logger using the common library:

```go
watermillLogger := log.NewWatermill(logrus.NewEntry(logrus.StandardLogger()))
```

</span>
	</div>
	</div>
