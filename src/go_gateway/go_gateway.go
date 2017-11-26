package main

import (
    "net/http"
    "log"
    "fmt"
    "github.com/streadway/amqp"
    "math/rand"
    "time"
)


func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	rand.Seed(time.Now().UTC().UnixNano())
    http.HandleFunc("/get", getHandler)
    http.HandleFunc("/set", setHandler)
    http.HandleFunc("/keys", keysHandler)
    err := http.ListenAndServe(":9090", nil) 
    if err != nil {
        log.Fatal("ListenAndServe: ", err)
    }	
}


func getHandler(w http.ResponseWriter, r *http.Request) {
    key := r.URL.Query().Get("key")
    jsonBody := fmt.Sprintf("{\"operation\": \"get\", \"key\": \"%s\"}", key)
    corrId := randomString(32)

    conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

 	q, err := ch.QueueDeclare(
  		"",    // name
  		false, // durable
  		false, // delete when usused
  		true,  // exclusive
  		false, // noWait
  		nil,   // arguments
	)

  	failOnError(err, "Failed to register a queue")	

 	err = ch.Publish(
	"",          // exchange
	"rpc_queue", // routing key
	false,       // mandatory
	false,       // immediate
	amqp.Publishing{
		ContentType:   "text/plain",
		CorrelationId: corrId,
		ReplyTo:       q.Name,
		Body:          []byte(jsonBody),
		})
	failOnError(err, "Failed to publish a message") 

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	response_chan := make(chan string)

	go func() {
		for d := range msgs {
			if corrId == d.CorrelationId {
				response_chan <- fmt.Sprintf("%s: %s", key, string(d.Body)) 
				break
			}
		}
	}()
	fmt.Fprintf(w, <- response_chan)
}

func setHandler(w http.ResponseWriter, r *http.Request) {
 	key := r.FormValue("key")
 	value := r.FormValue("value")
 	jsonBody := fmt.Sprintf("{\"operation\": \"set\", \"key\": \"%s\", \"value\": \"%s\"}", key, value)  
 	corrId := randomString(32)

 	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

 	q, err := ch.QueueDeclare(
  		"",    // name
  		false, // durable
  		false, // delete when usused
  		true,  // exclusive
  		false, // noWait
  		nil,   // arguments
	)

  	failOnError(err, "Failed to register a consumer")	

 	err = ch.Publish(
	"",          // exchange
	"rpc_queue", // routing key
	false,       // mandatory
	false,       // immediate
	amqp.Publishing{
		ContentType:   "text/plain",
		CorrelationId: corrId,
		ReplyTo:       q.Name,
		Body:          []byte(jsonBody),
		})
	failOnError(err, "Failed to publish a message")   
}

func keysHandler(w http.ResponseWriter, r *http.Request) {
    jsonBody := fmt.Sprintf("{\"operation\": \"keys\"}")
    corrId := randomString(32)

    conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

 	q, err := ch.QueueDeclare(
  		"",    // name
  		false, // durable
  		false, // delete when usused
  		true,  // exclusive
  		false, // noWait
  		nil,   // arguments
	)

  	failOnError(err, "Failed to register a queue")	

 	err = ch.Publish(
	"",          // exchange
	"rpc_queue", // routing key
	false,       // mandatory
	false,       // immediate
	amqp.Publishing{
		ContentType:   "text/plain",
		CorrelationId: corrId,
		ReplyTo:       q.Name,
		Body:          []byte(jsonBody),
		})
	failOnError(err, "Failed to publish a message") 

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	response_chan := make(chan string)

	go func() {
		for d := range msgs {
			if corrId == d.CorrelationId {
				response_chan <- string(d.Body) 
				break
			}
		}
	}()
	fmt.Fprintf(w, <- response_chan)
}

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}