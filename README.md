# Go Simulator — eventos de rotas, frete e rastreamento

Simulador em **Go 1.23**, Kafka e MongoDB, desenvolvido como exercício de arquitetura orientada a eventos. O consumidor recebe eventos `RouteCreated` e `DeliveryStarted`, grava rotas e envia eventos derivados aos tópicos de frete e simulação.

## Como funciona

1. `RouteCreated` valida ID, distância não negativa, lista de pontos e latitude/longitude.
2. O MongoDB salva/atualiza a rota e seu valor de frete.
3. O Kafka publica `FreightCalculated` e só então confirma o evento de origem.
4. `DeliveryStarted` recupera a rota e publica cada `DriverMoved` **em ordem**, com um segundo entre posições. O offset só é confirmado quando a simulação termina.

O processamento é sequencial por consumidor. Isso evita que o timeout de 500 ms interrompa uma simulação que emite um ponto por segundo, mas limita a vazão de rotas longas.

## Testes

```bash
go test ./...
go vet ./...
go build ./cmd/simulator
```

O GitHub Actions executa esses comandos automaticamente em PRs e na `main`. Há testes de serialização, validação de eventos, ordem de movimentos, fórmula de frete e configuração de inicialização. **Não há testes de integração Kafka/MongoDB** neste pipeline.

## Executar com infraestrutura

Variáveis de ambiente:

```dotenv
MONGO_URI=mongodb://usuario:senha@127.0.0.1:27017/routes?authSource=admin
KAFKA_BROKER=127.0.0.1:9092
KAFKA_ROUTE_TOPIC=route
KAFKA_FREIGHT_TOPIC=freight
KAFKA_SIMULATION_TOPIC=simulation
KAFKA_GROUP_ID=route-group
```

`MONGO_URI` é **obrigatória** e não existe mais URL com credenciais de exemplo embutida no binário. Guarde senhas fora do repositório. A execução real exige Kafka e MongoDB acessíveis. O `compose.yaml` usa `../mongodb` e `../kafka`, portanto **um clone isolado não sobe a stack** sem essas pastas irmãs.

```bash
go run ./cmd/simulator
```

## Garantias e limites

- O consumidor usa `FetchMessage` e `CommitMessages`: uma falha do processamento **não confirma o offset**.
- As publicações no Kafka têm timeout e propagam erros; a consulta Mongo também tem deadline.
- **Entrega pelo menos uma vez (at-least-once), não exatamente uma vez.** Falhas entre publicar e confirmar podem produzir eventos duplicados. Consumidores devem tratar idempotência.
- Eventos inválidos/paradas de dependências interrompem o processo, deixando a mensagem sem commit; ainda faltam retries com backoff e fila de mensagens inválidas (DLQ).
- Não existe homologação integrada ou operação de produção. As variáveis e credenciais do Compose são para desenvolvimento.

[Arquitetura](docs/ARQUITETURA.md) · [Especificação dos eventos](docs/projeto/ESPECIFICACAO.md) · [UML](docs/uml/README.md) · [Plano de testes](docs/testes/PLANO_DE_TESTES.md)
