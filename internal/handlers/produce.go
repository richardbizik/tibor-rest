package handlers

import (
	"io"
	"net/http"

	"github.com/twmb/franz-go/pkg/kgo"
)

func ProduceKafkaEventHandler(kafkaClient *kgo.Client) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.Write([]byte(err.Error()))
			w.WriteHeader(http.StatusExpectationFailed)
			return
		}

		kafkaClient.ProduceSync(r.Context(), kgo.StringRecord(string(body)))
	}
}
