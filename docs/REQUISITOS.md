# Requisitos — Go Simulator (v1.1.0)

## Funcionais

- **RF01:** receber `RouteCreated` de Kafka contendo ID, distância não negativa e uma ou mais coordenadas válidas.
- **RF02:** persistir/atualizar rota e preço no MongoDB antes de publicar `FreightCalculated`.
- **RF03:** receber `DeliveryStarted` e recuperar uma rota persistida.
- **RF04:** publicar `DriverMoved` seguindo a ordem das direções com intervalo de aproximadamente um segundo.
- **RF05:** confirmar offsets Kafka somente após a publicação integral dos eventos derivados.
- **RF06:** rejeitar eventos JSON inválidos, de tipo desconhecido ou com IDs ausentes.

## Não funcionais

- **RNF01:** operações MongoDB com `context.Context` e timeout, sem contexto nulo.
- **RNF02:** publicações Kafka com deadline e falha propagada ao consumidor.
- **RNF03:** exigir `MONGO_URI` de ambiente; não embutir senha de Mongo no executável.
- **RNF04:** interromper o consumo em SIGTERM/SIGINT e liberar conexões.
- **RNF05:** executar `go test ./...`, `go vet ./...` e `go build ./cmd/simulator` em CI.

## Restrições conhecidas

O sistema entrega mensagens **pelo menos uma vez** (pode repetir eventos), não tem DLQ, retry com backoff, outbox transacional nem testes E2E Mongo/Kafka. O consumo sequencial exige avaliação de throughput antes de uso real.

[Especificação dos eventos](projeto/ESPECIFICACAO.md) · [Plano de testes](testes/PLANO_DE_TESTES.md)

## Requisitos de robustez — GO-SIM v1.2.0

- **RNF06:** rejeitar eventos Kafka com mais de 262.144 bytes antes da decodificação JSON.
- **RNF07:** rejeitar rotas com zero ou mais de 600 coordenadas antes do upsert e da publicação.
- **RNF08:** validar também coordenadas provenientes do MongoDB antes de gerar eventos de movimento.
- **RNF09:** respeitar o cancelamento do contexto antes de iniciar o tratamento de um evento.

**Política de erro:** a versão não introduz DLQ e mantém entrega pelo menos uma vez (at-least-once). Mensagens inválidas interrompem o consumidor sem commit e podem exigir quarentena ou correção no produtor.
