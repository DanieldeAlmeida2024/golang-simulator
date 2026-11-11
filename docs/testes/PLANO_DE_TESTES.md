# Testes — Simulador Go de rotas, fretes e coordenadas

**Documento v1.0.0**, 11/11/2026, base `3aa74e2463f17d3138fd7a105809e7d6f20b9997`. Testes propostos não foram executados neste PR.

| ID | Ação | Critério de aceite | Situação |
|---|---|---|---|
| GO-01 | Calcular frete 0,1,120 | Fórmula e arredondamento corretos | Planejado |
| GO-02 | RouteCreated válido | Upsert Mongo + FreightCalculated no tópico freight | Planejado |
| GO-03 | DeliveryStarted válido | Emissão de um DriverMoved por coordenada | Planejado |
| GO-04 | Intervalo de 1s e timeout 500ms | Detectar terminação precoce do consumidor | Planejado |
| GO-05 | Evento desconhecido | HandleEvent retorna erro | Planejado |
| GO-06 | Mensagem JSON inválida | Erro de unmarshal controlado | Planejado |
| GO-07 | MongoDB indisponível | Erro sem confirmação indevida | Planejado |
| GO-08 | Falha do producer freight | Erro propagado, sem sucesso aparente | Planejado |
| GO-09 | Restart após leitura Kafka | Reprocessamento/idempotência sem perda | Planejado |
| GO-10 | SIGTERM durante a rota | Canais e writers finalizados | Planejado |
| GO-11 | Compose sem pastas irmãs | Erro de includes reconhecido | Planejado |
| GO-12 | Frete com distância negativa | Regra de negócio validada e documentada | Planejado |

## Execução e evidências
```bash
go test ./... && go vet ./...
```

Teste de unidade não substitui E2E. Para simulação distribuída, usar MongoDB/Kafka descartáveis e stubs de API; para frontend, testar socket com dados sintéticos. Guardar SHA, ambiente, entradas, retorno, logs sem segredo e resultado por ID. Casos P0 são bloqueantes até evidência explícita.
