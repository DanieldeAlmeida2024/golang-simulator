# Requisitos — Go Simulator (v1.1.0)

## Funcionais

- **RF01:** receber `RouteCreated` de Kafka contendo ID, distância não negativa e uma ou mais coordenadas válidas.
- **RF02:** persistir/atualizar rota e preço no MongoDB antes de publicar `FreightCalculated`.
- **RF03:** receber `DeliveryStarted` e recuperar uma rota persistida.
- **RF04:** publicar `DriverMoved` seguindo a ordem das direções com intervalo de aproximadamente um segundo.
- **RF05:** confirmar o offset de origem após confirmação de todos os eventos derivados pelo Kafka. Para eventos permanentemente inválidos, confirmar a origem após publicação confirmada na DLQ.
- **RF06:** classificar JSON inválido, eventos desconhecidos e IDs ausentes como falhas permanentes e encaminhá-los à DLQ.

## Não funcionais

- **RNF01:** operações MongoDB com `context.Context` e timeout, sem contexto nulo.
- **RNF02:** publicações Kafka com deadline e falha propagada ao consumidor.
- **RNF03:** exigir `MONGO_URI` de ambiente; não embutir senha de Mongo no executável.
- **RNF04:** interromper o consumo em SIGTERM/SIGINT e liberar conexões.
- **RNF05:** executar `go test ./...`, `go vet ./...` e `go build ./cmd/simulator` em CI.

## Restrições conhecidas

O sistema entrega mensagens **pelo menos uma vez** e pode repetir eventos. Reprocessamento com backoff no consumidor, outbox transacional e testes E2E Mongo/Kafka seguem pendentes. O consumo sequencial exige avaliação de vazão antes de uso real.

[Especificação dos eventos](projeto/ESPECIFICACAO.md) · [Plano de testes](testes/PLANO_DE_TESTES.md)

## Requisitos de robustez — GO-SIM v1.2.0

- **RNF06:** rejeitar eventos Kafka com mais de 262.144 bytes antes da decodificação JSON.
- **RNF07:** rejeitar rotas com zero ou mais de 600 coordenadas antes do upsert e da publicação.
- **RNF08:** validar também coordenadas provenientes do MongoDB antes de gerar eventos de movimento.
- **RNF09:** respeitar o cancelamento do contexto antes de iniciar o tratamento de um evento.

## Confirmação de publicações — GO-SIM v1.3.0

- **RNF10:** usar publicação Kafka síncrona e `RequireAll` nos tópicos de frete, simulação e DLQ.
- **RNF11:** preservar o offset de origem quando a publicação falhar. Erros transitórios permanecem no fluxo de reprocessamento.
