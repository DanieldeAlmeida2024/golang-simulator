# Simulador Go de rotas, fretes e coordenadas

**Documento v1.0.0**, 11/11/2026, base `3aa74e2463f17d3138fd7a105809e7d6f20b9997`. Testes propostos não foram executados neste PR.

## Contexto, regras e contratos
O processo em `cmd/simulator/main.go` é um consumidor Kafka Go 1.23.1, via `segmentio/kafka-go`, conectado ao MongoDB por `mongo-driver`. Lê `RouteCreated` e `DeliveryStarted` do tópico `route` e produz `FreightCalculated` em `freight` e `DriverMoved` em `simulation`.

`internal/handler.go` define os contratos reais: `RouteCreatedEvent{event,id,distance,directions[]}`, `DeliveryStartedEvent{event,route_id}`, `FreightCalculatedEvent{event,route_id,amount}`, `DriverMovedEvent{event,route_id,lat,lng}`. `EventHub.HandleEvent` decodifica o campo `event`, roteia os dois tipos admitidos e rejeita os demais. `RouteService.CreateRoute` grava via MongoDB upsert na coleção `routes.routes` e calcula `floor((distance*0.15 + 0.3)*100)/100`. A unidade de distância/moeda não está formalmente descrita no contrato.

Após `DeliveryStarted`, `DeliveryStartedHandler` percorre direções e envia um ponto ao canal a cada 1 segundo. O consumidor do canal em `EventHub` faz timeout de 500 ms: **pode encerrar antes do próximo ponto**, falha de desenho a comprovar por teste. O `ReadMessage` é processado em goroutine, expondo risco de commit antes de persistir/emitir evento. O serviço registra falhas de escrita Kafka e retorna sucesso em `handleRouteCreated` mesmo após falha de publicação.

O Compose inclui `../mongodb/docker-compose.yaml` e `../kafka/docker-compose.yaml`; portanto um clone isolado não sobe a stack. `events.json` guarda duas mensagens JSON consecutivas e não é um único documento JSON válido.

## Verificação do ambiente
```bash
go test ./... && go vet ./...
```

Consulte os [diagramas](../uml/README.md) e a [matriz de testes](../testes/PLANO_DE_TESTES.md); ambos devem ser mantidos sincronizados com o código.
