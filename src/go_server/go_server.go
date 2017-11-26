package main
import (
	"log"
    "strconv"
    "fmt"
	"github.com/streadway/amqp"
	"encoding/json"
	"zvelo.io/ttlru"
	"time"
	//"reflect"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

type Request struct {
    Operation string      `json:"operation"`
    Key string 			  `json:"key"`
    Value string		  `json:"value"`
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"rpc_queue", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

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

	forever := make(chan bool)

	cache := ttlru.New(100, ttlru.WithTTL(5 * time.Minute))

	//log.Println(reflect.TypeOf(ch))	

	for d := range msgs {    
    	go hitCache(cache, d)
    }	

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}


func hitCache(cache ttlru.Cache, d amqp.Delivery) {
	
	log.Printf("Received a message: %s", d.Body)
	data := Request{}
	err := json.Unmarshal(d.Body, &data)
	failOnError(err, "Failed to read json")

	switch data.Operation {
    	case "get":
    		conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
			failOnError(err, "Failed to connect to RabbitMQ")
		//	defer conn.Close()

			ch, err := conn.Channel()
			failOnError(err, "Failed to open a channel")
		//	defer ch.Close()

			failOnError(err, "Failed to declare a queue")

       		value, succ := cache.Get(data.Key)
			log.Printf(strconv.FormatBool(succ))
			log.Printf(value.(string))
			err = ch.Publish(
				"",        // exchange
				d.ReplyTo, // routing key
				false,     // mandatory
				false,     // immediate
				amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: d.CorrelationId,
					Body:          []byte(value.(string)),
			})
			failOnError(err, "Failed to publish a message")
    	case "set":
       		succ := cache.Set(data.Key, data.Value)
       		log.Printf(data.Key)
			log.Printf(strconv.FormatBool(succ))
       		log.Printf(strconv.Itoa(cache.Len()))
       	case "update":
       		succ := cache.Set(data.Key, data.Value)	
       		log.Printf(strconv.FormatBool(succ))
       	case "remove":
       		succ := cache.Del(data.Key)
       		log.Printf(strconv.FormatBool(succ))
       	case "keys":
       		keys := cache.Keys()
       		string_keys := make([]string, len(keys))
			for i, v := range keys {
    			string_keys[i] = fmt.Sprint(v)
			}
       		conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
			failOnError(err, "Failed to connect to RabbitMQ")
			defer conn.Close()

			ch, err := conn.Channel()
			failOnError(err, "Failed to open a channel")
			defer ch.Close()

			failOnError(err, "Failed to declare a queue")

			err = ch.Publish(
				"",        // exchange
				d.ReplyTo, // routing key
				false,     // mandatory
				false,     // immediate
				amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: d.CorrelationId,
					Body:          []byte(fmt.Sprintf("%#v\n", string_keys)),
			})
			failOnError(err, "Failed to publish a message")
       	default:
       		log.Printf("Unknown operation: %s", data.Operation)	
   	} 	
}