# Plano de testes — Go Simulator

**GO-SIM v1.1.0** · 14/11/2026

## Executados automaticamente pelo workflow `testes-go.yml`

```bash
go test ./...
go vet ./...
go build ./cmd/simulator
```

| ID | Condição | Esperado |
| --- | --- | --- |
| GO-U01 | Distâncias 0, 1, 2, 120 | Frete arredondado: 0.30, 0.45, 0.60, 18.30 |
| GO-U02 | Rota com três coordenadas | Três `DriverMoved` com ID/ordem preservados |
| GO-U03 | Rota sem ID, distância negativa, direções ausentes | Erro antes de qualquer chamada de banco |
| GO-U04 | Latitude fora do intervalo ou infinito | Erro de validação |
| GO-U05 | `DeliveryStarted` sem ID | Erro sem consulta Mongo |
| GO-U06 | JSON inválido ou evento desconhecido | Erro sem panic |
| GO-U07 | Serialização `DriverMoved` | Chaves JSON `event,route_id,lat,lng` |
| GO-U08 | `MONGO_URI` ausente | Inicialização falha sem acesso externo |
| GO-U09 | Variável de configuração vazia | Usa fallback definido |

## Pendências que exigem infraestrutura de integração

| ID | Condição | Resultado esperado |
| --- | --- | --- |
| GO-I01 | `RouteCreated` em Kafka real + Mongo | Rota gravada e um evento de frete |
| GO-I02 | Rota com três pontos | Três `DriverMoved` com intervalo de 1s |
| GO-I03 | Falha no writer antes de confirmar mensagem | Offset não confirmado e replay |
| GO-I04 | Falha de Mongo no upsert | Evento sem commit; erro visível |
| GO-I05 | Crash entre publicação e commit | Duplicata tratada pelo consumidor |
| GO-I06 | SIGTERM durante simulação | Shutdown, sem goroutines penduradas |
| GO-I07 | Mensagem inválida persistente | Política de DLQ implementada e testada com injeção de dependências; execução real em broker ainda pendente |
| GO-I08 | Compose com `../mongodb` e `../kafka` | Rede, portas e dependências verificadas |

**Importante:** a CI não valida as linhas GO-I. Nenhum teste de broker, banco real ou sistema externo deve ser reportado como aprovado sem logs e resultados de uma execução isolada.

## GO-SIM 1.2.0 — regressões de orçamento e falha fechada

| ID | Cenário | Aceitação |
| --- | --- | --- |
| GO-U10 | JSON com 262.145 bytes | Rejeitado antes de decodificar |
| GO-U11 | JSON com 262.144 bytes | Continua até validação JSON |
| GO-U12 | Rota com exatamente 600 pontos válidos | Validação aceita |
| GO-U13 | Rota sem pontos ou com 601 pontos | Rejeitada antes de tocar no MongoDB |
| GO-U14 | Latitude e longitude nos limites ±90 e ±180 | Aceitas; NaN, infinito e fora do intervalo rejeitados |
| GO-U15 | Contexto cancelado antes do evento | Rejeitado antes de persistência/publicação |

Todos são testes unitários de internal/handler_test.go, sem servidor Kafka/MongoDB real. A verificação completa da não confirmação de offsets, da duração de simulação e de falhas de publicação permanece no backlog de integração GO-I.

## GO-DLQ-01..05 — política de mensagens incompatíveis

| ID | Condição | Aceitação automatizada |
| --- | --- | --- |
| GO-DLQ-01 | Evento com JSON inválido, tipo desconhecido ou rota sem ID | Marcado como `ErrInvalidEvent` |
| GO-DLQ-02 | Mensagem incompatível com publicação confirmada em route-dlq | Depois, e somente depois, confirmação do offset |
| GO-DLQ-03 | Erro do writer da DLQ | Offset não confirmado |
| GO-DLQ-04 | Erro de Mongo/cancelamento/publicação normal | Não vai para DLQ e não confirma offset |
| GO-DLQ-05 | Evento válido | Commit normal sem produzir DLQ |

Testes isolados com callbacks, sem Kafka/Mongo reais. Duplicação da DLQ em crash entre publish e commit, políticas ACL/TLS/retenção, observabilidade e reprocessamento manual exigem homologação E2E.

## GO-SIM 1.3.0 — configuração das publicações

**GO-U16:** os produtores de frete, simulação e DLQ usam `RequireAll` e modo síncrono. O cenário `TestKafkaWriterExigeConfirmacaoSincrona` valida a configuração sem broker. Os testes GO-DLQ verificam separadamente a ordem entre publicação e commit e o comportamento em caso de erro.
