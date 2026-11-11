# Plano de testes — Go Simulator

> **GO-SIM v1.0.0** · 11/11/2026 · código `bb46a6831ff6c5586d69c1113c52e9dd6b5348a2` · análise estática; resultados pendentes.

## Cobertura observada
Não há `*_test.go` no inventário atual. O teste `tests/test_documentacao.py` verifica somente a documentação; não valida Go, Mongo ou Kafka.

| ID | Prioridade | Caso | Resultado esperado |
|---|---|---|---|
| GO-01 | P0 | FreightService 0,1,120 | Valor correto pela fórmula e arredondamento |
| GO-02 | P0 | RouteCreated JSON válido | Upsert e FreightCalculated com route_id |
| GO-03 | P0 | DeliveryStarted sem rota | Erro e nenhuma simulação |
| GO-04 | P0 | Evento desconhecido | Erro `unknown event type` |
| GO-05 | P0 | JSON malformado | Erro de deserialização sem crash |
| GO-06 | P0 | Mongo com contexto nil | Corrigir/validar uso de contexto e timeout |
| GO-07 | P0 | 3 direções com 1s de intervalo | 3 DriverMoved no tópico simulation |
| GO-08 | P0 | Kafka indisponível ao publicar frete | Falha observável/retry, sem perda silenciosa |
| GO-09 | P0 | Crash entre ReadMessage e HandleEvent | Replay sem perda de rota |
| GO-10 | P1 | Eventos concorrentes para mesma rota | Sem goroutines vazando e com ordem definida |
| GO-11 | P1 | SIGTERM durante simulação | Desligamento controlado de canais/brokers |
| GO-12 | P1 | Compose com dependências de pastas irmãs | Mongo/Kafka resolvidos por DNS |
| GO-13 | P1 | Distância negativa/grande | Política de limite explícita |

## Execução de referência
```bash
go test ./...
go vet ./...
go build ./cmd/simulator
docker compose config
```

Os testes de integração devem usar Mongo e Kafka isolados, confirmar quantos eventos chegaram e registrar SHA, ambientes, logs sem dados sensíveis e resultado. Os 13 casos acima permanecem **não executados** até produção de evidência.
