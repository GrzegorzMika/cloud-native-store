package main

import (
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

func main() {
	r := mux.NewRouter()
	store := NewInMemoryStore()
	logger, err := NewFileTransactionLogger("transaction.log")
	if err != nil {
		log.Panicf("failed to create event logger: %s", err)
	}
	defer logger.Close()

	err = initializeTransactionLog(logger, store)
	if err != nil {
		log.Panicf("failed to initialize transaction log: %s", err)
	}

	r.HandleFunc("/v1/{key}", keyValuePutHandler(store, logger)).Methods(http.MethodPut)
	r.HandleFunc("/v1/{key}", keyValueGetHandler(store, logger)).Methods(http.MethodGet)
	r.HandleFunc("/v1/{key}", keyValueDeleteHandler(store, logger)).Methods(http.MethodDelete)

	log.Println(http.ListenAndServe(":8080", r))
}
