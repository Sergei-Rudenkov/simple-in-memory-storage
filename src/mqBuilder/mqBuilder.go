package mqBuilder

import (
	"log"
    	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func ConnectMQ() (conn *amqp.Connection, ch *amqp.Channel){
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, errr := conn.Channel()
	failOnError(errr, "Failed to open a channel")

	return
}

func DeclareExchange(ch *amqp.Channel, name string, exchange_type string) {
	err := ch.ExchangeDeclare(
		name,   // name
		exchange_type, // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare an exchange")
}

func DeclareClientQueue(ch *amqp.Channel) (q amqp.Queue){
	q, err := ch.QueueDeclare(
  		"",    // name
  		false, // durable
  		false, // delete when usused
  		true,  // exclusive
  		false, // noWait
  		nil,   // arguments
	)

  	failOnError(err, "Failed to register a queue")	
  	return
}

func DeclareServerQueue(ch *amqp.Channel, q_Name string) (q amqp.Queue){
	q, err := ch.QueueDeclare(
  		q_Name, // name
  		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)

  	failOnError(err, "Failed to register a queue")	
  	return
}

func PublishQueue(ch *amqp.Channel, routing_key string, q_Name string, corrId string, jsonBody string) {
	err := ch.Publish(
		"",          // exchange
		routing_key, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: corrId,
			ReplyTo:       q_Name,
			Body:          []byte(jsonBody),
	})
	failOnError(err, "Failed to publish a message") 	
}

func PublishExchange(ch *amqp.Channel, exchange_name string, jsonBody string) {
	err := ch.Publish(
  		exchange_name, // exchange
  		"",     // routing key
  		false,  // mandatory
  		false,  // immediate
  		amqp.Publishing{
        	ContentType: "text/plain",
          	Body:        []byte(jsonBody),
  	})
  	failOnError(err, "Failed to publish a message") 
}	

func ConsumeQueue(ch *amqp.Channel, q_Name string) (<- chan amqp.Delivery) {
	msgs, err := ch.Consume(
		q_Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")	
	return msgs
}
