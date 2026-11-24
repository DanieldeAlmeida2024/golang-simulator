package main

import (
    "context"
    "errors"
    "reflect"
    "strings"
    "testing"

    "github.com/devfullcycle/imersao20/simulator/internal"
    "github.com/segmentio/kafka-go"
)

func TestServicoRecusaConfigSemMongoURI(t *testing.T) {
    t.Setenv("MONGO_URI", "")
    err := run()
    if err == nil || !strings.Contains(err.Error(), "MONGO_URI") {
        t.Fatalf("configuração sem URI deveria falhar explicitamente: %v", err)
    }
}

func TestGetEnvUsaFallbackParaVazio(t *testing.T) {
    t.Setenv("GO_SIM_TEST", "")
    if getEnv("GO_SIM_TEST", "padrao") != "padrao" {
        t.Fatal("variável vazia deve usar fallback")
    }
    t.Setenv("GO_SIM_TEST", "personalizado")
    if getEnv("GO_SIM_TEST", "padrao") != "personalizado" {
        t.Fatal("variável definida não foi aplicada")
    }
}


func TestKafkaWriterExigeConfirmacaoSincrona(t *testing.T) {
    for _, topic := range []string{"freight", "simulation", "route-dlq"} {
        t.Run(topic, func(t *testing.T) {
            writer := newKafkaWriter("localhost:9092", topic)
            if writer.Topic != topic || writer.RequiredAcks != kafka.RequireAll || writer.Async {
                t.Fatalf("topic=%q acks=%v async=%v", writer.Topic, writer.RequiredAcks, writer.Async)
            }
        })
    }
}

func TestDLQConfirmaSomenteAposPublicacao(t *testing.T) {
    message := kafka.Message{
        Topic: "route", Partition: 3, Offset: 71,
        Key: []byte("route-42"), Value: []byte("not-json"),
    }
    calls := []string{}
    var quarantined kafka.Message
    didDLQ, err := processKafkaRecord(context.Background(), message,
        func(_ context.Context, body []byte) error {
            calls = append(calls, "handle")
            if !reflect.DeepEqual(body, message.Value) { t.Fatal("payload alterado") }
            return internal.ErrInvalidEvent
        },
        func(_ context.Context, msg kafka.Message) error {
            calls = append(calls, "dlq")
            quarantined = msg
            return nil
        },
        func(_ context.Context, msg kafka.Message) error {
            calls = append(calls, "commit")
            if msg.Offset != message.Offset { t.Fatal("offset alterado") }
            return nil
        },
    )
    if err != nil || !didDLQ {
        t.Fatalf("permanent invalid event should be quarantined and acked: %v", err)
    }
    if !reflect.DeepEqual(calls, []string{"handle", "dlq", "commit"}) {
        t.Fatalf("ordem errada: %v", calls)
    }
    if string(quarantined.Value) != "not-json" || string(quarantined.Key) != "route-42" {
        t.Fatal("DLQ não preservou mensagem original")
    }
    headers := map[string]string{}
    for _, h := range quarantined.Headers { headers[h.Key] = string(h.Value) }
    if headers["source-topic"] != "route" ||
       headers["source-partition"] != "3" || headers["source-offset"] != "71" {
        t.Fatalf("metadados de rastreabilidade ausentes: %v", headers)
    }
}

func TestFalhaDaDLQNaoConfirmaMensagemOriginal(t *testing.T) {
    commits := 0
    didDLQ, err := processKafkaRecord(context.Background(), kafka.Message{},
        func(context.Context, []byte) error { return internal.ErrInvalidEvent },
        func(context.Context, kafka.Message) error { return errors.New("Kafka offline") },
        func(context.Context, kafka.Message) error { commits++; return nil },
    )
    if err == nil || didDLQ || commits != 0 {
        t.Fatalf("DLQ offline must leave original offset uncommitted: %v", err)
    }
}

func TestFalhaTransitoriaNaoVaiParaDLQNemConfirma(t *testing.T) {
    sent, commits := 0, 0
    didDLQ, err := processKafkaRecord(context.Background(), kafka.Message{},
        func(context.Context, []byte) error { return errors.New("Mongo offline") },
        func(context.Context, kafka.Message) error { sent++; return nil },
        func(context.Context, kafka.Message) error { commits++; return nil },
    )
    if err == nil || didDLQ || sent != 0 || commits != 0 {
        t.Fatalf("failure must remain uncommitted and not enter DLQ: %v", err)
    }
}

func TestMensagemValidaNaoPublicaDLQ(t *testing.T) {
    sent, commits := 0, 0
    didDLQ, err := processKafkaRecord(context.Background(), kafka.Message{},
        func(context.Context, []byte) error { return nil },
        func(context.Context, kafka.Message) error { sent++; return nil },
        func(context.Context, kafka.Message) error { commits++; return nil },
    )
    if err != nil || didDLQ || sent != 0 || commits != 1 {
        t.Fatalf("valid event must commit once: %v", err)
    }
}
