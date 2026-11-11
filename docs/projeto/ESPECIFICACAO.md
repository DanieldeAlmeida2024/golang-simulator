# Go Simulator — especificação de eventos e entregas

> **GO-SIM v1.0.0** · 11/11/2026 · código `bb46a6831ff6c5586d69c1113c52e9dd6b5348a2` · análise estática; resultados pendentes.

## Objetivo
Consumir `route` no Kafka, persistir rotas/preços em MongoDB e produzir `freight` e `simulation`. Stack real: Go 1.23.1, `segmentio/kafka-go`, `mongo-driver`. Entradas: `cmd/simulator/main.go`, `internal/eventhub.go`, `internal/handler.go`, `internal/route.go`.

## Contratos JSON
| Evento | Direção | Campos | Efeito |
|---|---|---|---|
| RouteCreated | entrada route | `event,id,distance,directions[{lat,lng}]` | Salva Route e calcula frete |
| DeliveryStarted | entrada route | `event,route_id` | Busca rota e simula deslocamento |
| FreightCalculated | saída freight | `event,route_id,amount` | Publica preço |
| DriverMoved | saída simulation | `event,route_id,lat,lng` | Publica ponto percorrido |

`EventHub.HandleEvent` rejeita tipos desconhecidos e JSON inválido. `events.json` contém dois objetos em sequência; portanto **não é um único JSON válido**, apenas exemplos separados.

## Fórmula e persistência
`FreightService.Calculate(distance int)` aplica `floor((distance * 0.15 + 0.3)*100)/100`. Unidade de distância e moeda não estão formalmente declaradas como contrato de negócio. `RouteService.CreateRoute` usa upsert na coleção `routes.routes` para os campos `_id`, `distance`, `directions`, `freight_price`. `GetRoute` recupera por ID. As chamadas MongoDB recebem `nil` em vez de `context.Context` com timeout; comportamento precisa ser validado/corrigido.

## Simulação temporal
`DeliveryStartedHandler` emite um `DriverMoved` por coordenada com pausa de **1 segundo**. O consumidor do canal em `EventHub` encerra a goroutine após **500 ms** sem mensagem. Isso pode cortar a simulação antes do próximo ponto; é um defeito potencial verificável por teste.

## Execução
`compose.yaml` inclui `../mongodb/docker-compose.yaml` e `../kafka/docker-compose.yaml`; não funciona em clone isolado sem as pastas irmãs. Principais variáveis: `MONGO_URI`, `KAFKA_BROKER`, `KAFKA_ROUTE_TOPIC`, `KAFKA_FREIGHT_TOPIC`, `KAFKA_SIMULATION_TOPIC`, `KAFKA_GROUP_ID`. Padrões Docker: `mongo:27017`, `kafka:9092`.

## Riscos de confiabilidade
- A API Kafka `ReadMessage` é seguida de goroutine para tratar evento; o offset pode ser confirmado antes de processamento efetivo.
- Falha em publicar FreightCalculated é apenas impressa e o handler retorna nil, criando sucesso aparente.
- Não há outbox transacional Mongo/Kafka e nem rotina de shutdown com drain documentada.
- Não usar logs de payload com dados pessoais ou credenciais.
