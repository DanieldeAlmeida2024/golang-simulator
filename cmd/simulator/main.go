package main

import (
    "context"
    "errors"
    "fmt"
    "log"
    "os"
    "os/signal"
    "strings"
    "syscall"
    "time"

    "github.com/devfullcycle/imersao20/simulator/internal"
    "github.com/segmentio/kafka-go"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
    "go.mongodb.org/mongo-driver/mongo/readpref"
)

func main() {
    if err := run(); err != nil {
        log.Fatal(err)
    }
}

func run() error {
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    mongoURI := strings.TrimSpace(os.Getenv("MONGO_URI"))
    if mongoURI == "" {
        return errors.New("MONGO_URI é obrigatória; configure o ambiente antes de iniciar")
    }
    kafkaBroker := getEnv("KAFKA_BROKER", "kafka:9092")
    kafkaRouteTopic := getEnv("KAFKA_ROUTE_TOPIC", "route")
    kafkaFreightTopic := getEnv("KAFKA_FREIGHT_TOPIC", "freight")
    kafkaSimulationTopic := getEnv("KAFKA_SIMULATION_TOPIC", "simulation")
    kafkaGroupID := getEnv("KAFKA_GROUP_ID", "route-group")
    kafkaDLQTopic := getEnv("KAFKA_DLQ_TOPIC", "route-dlq")
    if kafkaDLQTopic == kafkaRouteTopic || kafkaDLQTopic == kafkaFreightTopic ||
        kafkaDLQTopic == kafkaSimulationTopic {
        return errors.New("KAFKA_DLQ_TOPIC deve ser diferente dos tópicos de entrada e saída")
    }

    connectCtx, cancelConnect := context.WithTimeout(ctx, 10*time.Second)
    mongoConnection, err := mongo.Connect(connectCtx, options.Client().ApplyURI(mongoURI))
    if err != nil {
        cancelConnect()
        return fmt.Errorf("conectar ao MongoDB: %w", err)
    }
    if err := mongoConnection.Ping(connectCtx, readpref.Primary()); err != nil {
        cancelConnect()
        closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
        _ = mongoConnection.Disconnect(closeCtx)
        closeCancel()
        return fmt.Errorf("MongoDB não está pronto: %w", err)
    }
    cancelConnect()
    defer func() {
        closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        if err := mongoConnection.Disconnect(closeCtx); err != nil {
            log.Printf("erro ao encerrar MongoDB: %v", err)
        }
    }()

    freightWriter := &kafka.Writer{
        Addr: kafka.TCP(kafkaBroker),
        Topic: kafkaFreightTopic,
        Balancer: &kafka.LeastBytes{},
    }
    defer freightWriter.Close()

    simulationWriter := &kafka.Writer{
        Addr: kafka.TCP(kafkaBroker),
        Topic: kafkaSimulationTopic,
        Balancer: &kafka.LeastBytes{},
    }
    defer simulationWriter.Close()

    // Quarentena de eventos permanentes, nunca dos erros de Mongo/rede/publicação.
    deadLetterWriter := &kafka.Writer{
        Addr: kafka.TCP(kafkaBroker),
        Topic: kafkaDLQTopic,
        Balancer: &kafka.LeastBytes{},
    }
    defer deadLetterWriter.Close()

    routeReader := kafka.NewReader(kafka.ReaderConfig{
        Brokers: []string{kafkaBroker},
        Topic: kafkaRouteTopic,
        GroupID: kafkaGroupID,
    })
    defer routeReader.Close()

    routeService := internal.NewRouteService(mongoConnection, internal.NewFreightService())
    hub := internal.NewEventHub(routeService, freightWriter, simulationWriter)
    log.Printf("consumindo eventos do tópico %s", kafkaRouteTopic)

    for {
        message, err := routeReader.FetchMessage(ctx)
        if errors.Is(err, context.Canceled) {
            return nil // SIGINT/SIGTERM: fechar conexões de maneira ordenada
        }
        if err != nil {
            return fmt.Errorf("buscar evento Kafka: %w", err)
        }

        // A origem só é confirmada após processamento OU publicação confirmada na DLQ.
        // Erros transitórios de Mongo/Kafka permanecem sem commit para replay.
        quarantined, err := processKafkaRecord(ctx, message, hub.HandleEvent,
            func(ctx context.Context, msg kafka.Message) error {
                writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
                defer cancel()
                return deadLetterWriter.WriteMessages(writeCtx, msg)
            },
            func(ctx context.Context, msg kafka.Message) error {
                return routeReader.CommitMessages(ctx, msg)
            })
        if err != nil {
            return fmt.Errorf("processar evento Kafka partition=%d offset=%d: %w",
                message.Partition, message.Offset, err)
        }
        if quarantined {
            log.Printf("evento incompatível isolado em %s partition=%d offset=%d",
                kafkaDLQTopic, message.Partition, message.Offset)
        }
    }
}

func getEnv(key, fallback string) string {
    if value, exists := os.LookupEnv(key); exists && strings.TrimSpace(value) != "" {
        return value
    }
    return fallback
}


// processKafkaRecord separates permanent message defects from transient service failures.
// An invalid message is acknowledged only after the DLQ broker confirms the write.
func processKafkaRecord(
    ctx context.Context,
    message kafka.Message,
    handle func(context.Context, []byte) error,
    publishDLQ func(context.Context, kafka.Message) error,
    commit func(context.Context, kafka.Message) error,
) (bool, error) {
    err := handle(ctx, message.Value)
    if err != nil {
        if !errors.Is(err, internal.ErrInvalidEvent) {
            return false, err
        }
        quarantined := kafka.Message{
            Key: append([]byte(nil), message.Key...),
            Value: append([]byte(nil), message.Value...),
            Headers: []kafka.Header{
                {Key: "source-topic", Value: []byte(message.Topic)},
                {Key: "source-partition", Value: []byte(fmt.Sprint(message.Partition))},
                {Key: "source-offset", Value: []byte(fmt.Sprint(message.Offset))},
            },
        }
        if writeErr := publishDLQ(ctx, quarantined); writeErr != nil {
            return false, fmt.Errorf("DLQ indisponível, offset preservado: %w", writeErr)
        }
        if commitErr := commit(ctx, message); commitErr != nil {
            return false, fmt.Errorf("DLQ publicada, offset sem confirmação: %w", commitErr)
        }
        return true, nil
    }
    if commitErr := commit(ctx, message); commitErr != nil {
        return false, fmt.Errorf("offset não confirmado: %w", commitErr)
    }
    return false, nil
}
