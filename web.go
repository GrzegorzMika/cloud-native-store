package main

import (
	"errors"
	"io"
	"net/http"

	"github.com/gorilla/mux"
)

func keyValuePutHandler(store Store, logger TransactionLogger) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		key := vars["key"]
		value, err := io.ReadAll(r.Body)
		defer r.Body.Close()

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		err = store.Put(key, string(value))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		logger.WritePut(key, string(value))
		w.WriteHeader(http.StatusCreated)
	}
}
func keyValueGetHandler(store Store, _ TransactionLogger) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		key := vars["key"]

		value, err := store.Get(key)
		if errors.Is(err, ErrNoSuchKey) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		_, _ = w.Write([]byte(value))
	}
}
func keyValueDeleteHandler(store Store, logger TransactionLogger) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		key := vars["key"]

		err := store.Delete(key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		logger.WriteDelete(key)
		w.WriteHeader(http.StatusNoContent)
	}
}
