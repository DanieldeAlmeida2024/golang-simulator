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

    connectCtx, cancelConnect := context.WithTimeout(ctx, 10*time.Second)
    mongoConnection, err := mongo.Connect(connectCtx, options.Client().ApplyURI(mongoURI))
    if err != nil {
        cancelConnect()
        return fmt.Errorf("conectar ao MongoDB: %w", err)
    }
    if err := mongoConnection.Ping(connectCtx, readpref.Primary()); err != nil {
        cancelConnect()
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

        // Não confirmar offset antes de Mongo e publicações serem concluídos.
        // Falhas encerram o processo sem commit; replay é at-least-once.
        if err := hub.HandleEvent(ctx, message.Value); err != nil {
            return fmt.Errorf("processar evento Kafka partition=%d offset=%d: %w",
                message.Partition, message.Offset, err)
        }
        if err := routeReader.CommitMessages(ctx, message); err != nil {
            return fmt.Errorf("confirmar evento Kafka partition=%d offset=%d: %w",
                message.Partition, message.Offset, err)
        }
    }
}

func getEnv(key, fallback string) string {
    if value, exists := os.LookupEnv(key); exists && strings.TrimSpace(value) != "" {
        return value
    }
    return fallback
}
