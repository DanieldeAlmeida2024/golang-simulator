package internal

import (
    "context"
    "encoding/json"
    "errors"
    "math"
    "strings"
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

func TestLimiteDeDirecoesSemAcessoAoBanco(t *testing.T) {
    valid := make([]Directions, MaxRoutePoints)
    for i := range valid {
        valid[i] = Directions{Lat: -16.68, Lng: -49.26}
    }
    if err := validateDirections(valid); err != nil {
        t.Fatalf("route at declared limit must be accepted: %v", err)
    }
    for _, directions := range [][]Directions{nil, {}, append(valid, Directions{Lat: 0, Lng: 0})} {
        if err := validateDirections(directions); err == nil {
            t.Fatal("empty or oversized route accepted")
        }
    }
    event := NewRouteCreatedEvent("route-bounded", 500, append(valid, Directions{Lat: 0, Lng: 0}))
    if _, err := RouteCreatedHandler(context.Background(), event, nil); err == nil || !strings.Contains(err.Error(), "600") {
        t.Fatalf("oversized event should be rejected before touching Mongo: %v", err)
    }
}

func TestCoordenadasDosPontosSaoVerificadasNasRotas(t *testing.T) {
    cases := []Directions{
        {Lat: -90, Lng: 180},
        {Lat: 90, Lng: -180},
    }
    if err := validateDirections(cases); err != nil {
        t.Fatalf("valid boundaries rejected: %v", err)
    }
    for _, point := range []Directions{
        {Lat: math.NaN(), Lng: 0}, {Lat: 0, Lng: math.Inf(1)},
        {Lat: 0, Lng: -181}, {Lat: 91, Lng: 0},
    } {
        if err := validateDirections([]Directions{point}); err == nil {
            t.Fatalf("invalid coordinate accepted: %+v", point)
        }
    }
}

func TestHubRejeitaEventoGrandeAntesDeDecodificarOuPublicar(t *testing.T) {
    hub := &EventHub{}
    oversized := make([]byte, MaxEventBytes+1)
    if err := hub.HandleEvent(context.Background(), oversized); err == nil ||
        !strings.Contains(err.Error(), "exceeds") {
        t.Fatalf("oversized Kafka event was accepted: %v", err)
    }
    // Exact boundary still reaches JSON validation instead of size rejection.
    boundary := make([]byte, MaxEventBytes)
    err := hub.HandleEvent(context.Background(), boundary)
    if err == nil || strings.Contains(err.Error(), "exceeds") {
        t.Fatalf("exact boundary should fail on invalid JSON, not size: %v", err)
    }
}

func TestHubRespeitaCancelamentoAntesDeProcessar(t *testing.T) {
    ctx, cancel := context.WithCancel(context.Background())
    cancel()
    hub := &EventHub{}
    if err := hub.HandleEvent(ctx, []byte(`{"event":"RouteCreated"}`)); err == nil {
        t.Fatal("canceled context was accepted")
    }
}


func TestInvalidEventClassificationIsPermanentOnlyForContract(t *testing.T) {
    hub := &EventHub{}
    cases := [][]byte{
        []byte("{"),
        []byte(`{"event":"unknown"}`),
        []byte(`{"event":"RouteCreated","id":"","distance":1,"directions":[{"lat":0,"lng":0}]}`),
        []byte(`{"event":"DeliveryStarted","route_id":""}`),
        make([]byte, MaxEventBytes+1),
    }
    for _, body := range cases {
        err := hub.HandleEvent(context.Background(), body)
        if !errors.Is(err, ErrInvalidEvent) {
            t.Fatalf("invalid message must classify as permanent: %v", err)
        }
    }
    ctx, cancel := context.WithCancel(context.Background())
    cancel()
    if errors.Is(hub.HandleEvent(ctx, []byte(`{"event":"RouteCreated"}`)), ErrInvalidEvent) {
        t.Fatal("cancellation is transient; must not be silently quarantined")
    }
}
