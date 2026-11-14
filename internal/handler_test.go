package internal

import (
    "context"
    "encoding/json"
    "math"
    "testing"
)

func TestMovimentosPreservamRotaEOrdem(t *testing.T) {
    route := NewRoute("rota-1", 120, []Directions{
        {Lat: -16.6869, Lng: -49.2648},
        {Lat: -15.7942, Lng: -47.8822},
        {Lat: -23.5505, Lng: -46.6333},
    })
    result := BuildDriverMovementEvents(route)
    if len(result) != 3 {
        t.Fatalf("esperados 3 movimentos, recebidos %d", len(result))
    }
    for i, ev := range result {
        if ev.EventName != "DriverMoved" || ev.RouteID != route.ID {
            t.Fatalf("evento inválido em %d: %+v", i, ev)
        }
        if ev.Lat != route.Directions[i].Lat || ev.Lng != route.Directions[i].Lng {
            t.Fatalf("ordem ou posição alterada em %d: %+v", i, ev)
        }
    }
}

func TestRouteCreatedRejeitaDadosInvalidosAntesDoBanco(t *testing.T) {
    casos := []RouteCreatedEvent{
        {RouteID: "", Distance: 100, Directions: []Directions{{Lat: 0, Lng: 0}}},
        {RouteID: "rota", Distance: -1, Directions: []Directions{{Lat: 0, Lng: 0}}},
        {RouteID: "rota", Distance: 10, Directions: nil},
        {RouteID: "rota", Distance: 10, Directions: []Directions{{Lat: 91, Lng: 0}}},
        {RouteID: "rota", Distance: 10, Directions: []Directions{{Lat: -15, Lng: math.Inf(1)}}},
    }
    for i, evento := range casos {
        if _, err := RouteCreatedHandler(context.Background(), &evento, nil); err == nil {
            t.Errorf("caso inválido %d foi aceito", i)
        }
    }
}

func TestDeliveryStartedSemIDNaoConsultaBanco(t *testing.T) {
    if _, err := DeliveryStartedHandler(context.Background(), NewDeliveryStartedEvent(""), nil); err == nil {
        t.Fatal("ID vazio deveria ser rejeitado")
    }
}

func TestHubRejeitaJSONETipoInvalidos(t *testing.T) {
    eh := &EventHub{}
    casos := [][]byte{
        []byte("{"),
        []byte(`{"event":"EventoDesconhecido"}`),
        []byte(`{"event":"RouteCreated","id":"","distance":1,"directions":[{"lat":0,"lng":0}]}`),
        []byte(`{"event":"DeliveryStarted","route_id":""}`),
    }
    for _, payload := range casos {
        if err := eh.HandleEvent(context.Background(), payload); err == nil {
            t.Errorf("payload inválido foi aceito: %s", payload)
        }
    }
}

func TestContratosJSON(t *testing.T) {
    evento := NewDriverMovedEvent("rota", -16.68, -49.26)
    b, err := json.Marshal(evento)
    if err != nil {
        t.Fatal(err)
    }
    var doc map[string]interface{}
    if err := json.Unmarshal(b, &doc); err != nil {
        t.Fatal(err)
    }
    if doc["event"] != "DriverMoved" || doc["route_id"] != "rota" {
        t.Fatalf("contrato JSON quebrado: %s", b)
    }
}
