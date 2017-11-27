package main

import (
    "net/http"
    "log"
    "fmt"
    _ "github.com/streadway/amqp"
    "math/rand"
    "time"
    "mqBuilder"
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

    failOnError(err, "Failed to ListenAndServe")
}

func getHandler(w http.ResponseWriter, r *http.Request) {
    key := r.URL.Query().Get("key")
    jsonBody := fmt.Sprintf("{\"operation\": \"get\", \"key\": \"%s\"}", key)
    corrId := randomString(32)

    conn, ch := mqBuilder.ConnectMQ()
	defer conn.Close()
	defer ch.Close()

 	q := mqBuilder.DeclareClientQueue(ch);

 	mqBuilder.PublishQueue(ch, "rpc_queue", q.Name, corrId, jsonBody)

	msgs := mqBuilder.ConsumeQueue(ch, q.Name) 

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

    conn, ch := mqBuilder.ConnectMQ()
	defer conn.Close()
	defer ch.Close()

 	q := mqBuilder.DeclareClientQueue(ch);

 	mqBuilder.PublishQueue(ch, "rpc_queue", q.Name, corrId, jsonBody)
}

func keysHandler(w http.ResponseWriter, r *http.Request) {
    jsonBody := fmt.Sprintf("{\"operation\": \"keys\"}")
    corrId := randomString(32)

    conn, ch := mqBuilder.ConnectMQ()
	defer conn.Close()
	defer ch.Close()

 	q := mqBuilder.DeclareClientQueue(ch);	

 	mqBuilder.PublishQueue(ch, "rpc_queue", q.Name, corrId, jsonBody)

 	msgs := mqBuilder.ConsumeQueue(ch, q.Name)

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