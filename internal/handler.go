package internal

import (
    "context"
    "errors"
    "fmt"
    "math"
    "strings"
)

type RouteCreatedEvent struct {
    EventName  string       `json:"event"`
    RouteID    string       `json:"id"`
    Distance   int          `json:"distance"`
    Directions []Directions `json:"directions"`
}

func NewRouteCreatedEvent(routeID string, distance int, directions []Directions) *RouteCreatedEvent {
    return &RouteCreatedEvent{
        EventName: "RouteCreated", RouteID: routeID,
        Distance: distance, Directions: directions,
    }
}

type FreightCalculatedEvent struct {
    EventName string  `json:"event"`
    RouteID   string  `json:"route_id"`
    Amount    float64 `json:"amount"`
}

func NewFreightCalculatedEvent(routeID string, amount float64) *FreightCalculatedEvent {
    return &FreightCalculatedEvent{EventName: "FreightCalculated", RouteID: routeID, Amount: amount}
}

type DeliveryStartedEvent struct {
    EventName string `json:"event"`
    RouteID   string `json:"route_id"`
}

func NewDeliveryStartedEvent(routeID string) *DeliveryStartedEvent {
    return &DeliveryStartedEvent{EventName: "DeliveryStarted", RouteID: routeID}
}

type DriverMovedEvent struct {
    EventName string  `json:"event"`
    RouteID   string  `json:"route_id"`
    Lat       float64 `json:"lat"`
    Lng       float64 `json:"lng"`
}

func NewDriverMovedEvent(routeID string, lat float64, lng float64) *DriverMovedEvent {
    return &DriverMovedEvent{EventName: "DriverMoved", RouteID: routeID, Lat: lat, Lng: lng}
}

// RouteCreatedHandler valida o evento antes de tocar no banco de dados.
func RouteCreatedHandler(ctx context.Context, event *RouteCreatedEvent, routeService *RouteService) (*FreightCalculatedEvent, error) {
    if event == nil || strings.TrimSpace(event.RouteID) == "" {
        return nil, errors.New("RouteCreated: id da rota obrigatório")
    }
    if event.Distance < 0 || len(event.Directions) == 0 {
        return nil, errors.New("RouteCreated: distância negativa ou lista de direções vazia")
    }
    for i, ponto := range event.Directions {
        if math.IsNaN(ponto.Lat) || math.IsInf(ponto.Lat, 0) || math.Abs(ponto.Lat) > 90 ||
           math.IsNaN(ponto.Lng) || math.IsInf(ponto.Lng, 0) || math.Abs(ponto.Lng) > 180 {
            return nil, fmt.Errorf("RouteCreated: coordenada inválida no ponto %d", i)
        }
    }
    route := NewRoute(event.RouteID, event.Distance, event.Directions)
    routeCreated, err := routeService.CreateRoute(ctx, route)
    if err != nil {
        return nil, err
    }
    return NewFreightCalculatedEvent(routeCreated.ID, routeCreated.FreightPrice), nil
}

// BuildDriverMovementEvents preserva a ordem de cada coordenada, sem goroutines.
func BuildDriverMovementEvents(route Route) []*DriverMovedEvent {
    movimentos := make([]*DriverMovedEvent, 0, len(route.Directions))
    for _, direction := range route.Directions {
        movimentos = append(movimentos, NewDriverMovedEvent(route.ID, direction.Lat, direction.Lng))
    }
    return movimentos
}

func DeliveryStartedHandler(ctx context.Context, event *DeliveryStartedEvent, routeService *RouteService) ([]*DriverMovedEvent, error) {
    if event == nil || strings.TrimSpace(event.RouteID) == "" {
        return nil, errors.New("DeliveryStarted: id da rota obrigatório")
    }
    route, err := routeService.GetRoute(ctx, event.RouteID)
    if err != nil {
        return nil, err
    }
    return BuildDriverMovementEvents(route), nil
}
