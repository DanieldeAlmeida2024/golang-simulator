package internal

import (
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "time"

    "github.com/segmentio/kafka-go"
)

// EventHub só retorna sucesso depois de publicar todos os eventos derivados.
type EventHub struct {
    routeService     *RouteService
    freightWriter    *kafka.Writer
    simulationWriter *kafka.Writer
}

func NewEventHub(routeService *RouteService, freightWriter *kafka.Writer, simulationWriter *kafka.Writer) *EventHub {
    return &EventHub{
        routeService: routeService,
        freightWriter: freightWriter,
        simulationWriter: simulationWriter,
    }
}

func (eh *EventHub) HandleEvent(ctx context.Context, msg []byte) error {
    var baseEvent struct {
        EventName string `json:"event"`
    }
    if err := json.Unmarshal(msg, &baseEvent); err != nil {
        return fmt.Errorf("invalid JSON event: %w", err)
    }

    switch baseEvent.EventName {
    case "RouteCreated":
        var event RouteCreatedEvent
        if err := json.Unmarshal(msg, &event); err != nil {
            return fmt.Errorf("invalid RouteCreated payload: %w", err)
        }
        return eh.handleRouteCreated(ctx, event)
    case "DeliveryStarted":
        var event DeliveryStartedEvent
        if err := json.Unmarshal(msg, &event); err != nil {
            return fmt.Errorf("invalid DeliveryStarted payload: %w", err)
        }
        return eh.handleDeliveryStarted(ctx, event)
    default:
        return errors.New("unknown event type")
    }
}

func (eh *EventHub) handleRouteCreated(ctx context.Context, event RouteCreatedEvent) error {
    freight, err := RouteCreatedHandler(ctx, &event, eh.routeService)
    if err != nil {
        return fmt.Errorf("create route and freight: %w", err)
    }
    value, err := json.Marshal(freight)
    if err != nil {
        return fmt.Errorf("serialize freight event: %w", err)
    }

    writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
    defer cancel()
    if err := eh.freightWriter.WriteMessages(writeCtx, kafka.Message{
        Key: []byte(freight.RouteID), Value: value,
    }); err != nil {
        return fmt.Errorf("publish FreightCalculated to kafka: %w", err)
    }
    return nil
}

func (eh *EventHub) handleDeliveryStarted(ctx context.Context, event DeliveryStartedEvent) error {
    movements, err := DeliveryStartedHandler(ctx, &event, eh.routeService)
    if err != nil {
        return fmt.Errorf("load route for simulation: %w", err)
    }

    for index, moved := range movements {
        value, err := json.Marshal(moved)
        if err != nil {
            return fmt.Errorf("serialize DriverMoved: %w", err)
        }
        writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
        err = eh.simulationWriter.WriteMessages(writeCtx, kafka.Message{
            Key: []byte(moved.RouteID), Value: value,
        })
        cancel()
        if err != nil {
            return fmt.Errorf("publish DriverMoved point %d: %w", index, err)
        }

        if index < len(movements)-1 {
            timer := time.NewTimer(time.Second)
            select {
            case <-timer.C:
            case <-ctx.Done():
                timer.Stop()
                return ctx.Err()
            }
        }
    }
    return nil
}
