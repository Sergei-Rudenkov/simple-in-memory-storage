# Simple in memory storage.

### Tech 

Application uses following opensourse projects:
* [RabbitMQ] - MQ brocker 
* [zvelo/ttlru] - Open source Package ttlru provides a simple, goroutine safe, cache with a fixed number of entries. Each entry has a per-cache defined TTL. 

| Action | Endpoint |
| ------ | ------ |
| Set | /set {key: "hello", value:"world" } | 
| Get | /get?key=hello |
| Remove | /remove?key=hello  |
| Keys | /keys |



Application supports serverside scaling by adding instances of go_server replicas. Cache with event driven model



![architecture](https://i.stack.imgur.com/BTvYl.png)

### Installation

1. Go-install `github.com/streadway/amqp` and zvelo.io/ttlru
2. Run `RabbitMQ` against default port
3. Go-run `go_gateway.go` & `go_server.go`

[RabbitMQ]: <https://www.rabbitmq.com/>
[zvelo/ttlru]: <https://github.com/zvelo/ttlru>


