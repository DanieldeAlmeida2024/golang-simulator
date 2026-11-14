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
| GO-I07 | Mensagem inválida persistente | Política futura de DLQ/retry definida |
| GO-I08 | Compose com `../mongodb` e `../kafka` | Rede, portas e dependências verificadas |

**Importante:** a CI não valida as linhas GO-I. Nenhum teste de broker, banco real ou sistema externo deve ser reportado como aprovado sem logs e resultados de uma execução isolada.
