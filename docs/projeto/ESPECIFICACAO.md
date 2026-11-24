# Go Simulator — contratos de eventos e garantias

**GO-SIM v1.1.0** · 14/11/2026 · implementação em `cmd/simulator` e `internal/`.

## Objetivo

Consumir eventos `route` no Kafka, salvar rotas/fretes no MongoDB e produzir eventos de frete e simulação na ordem esperada.

## JSON de eventos

| Evento | Tópico | Campos | Efeito |
| --- | --- | --- | --- |
| RouteCreated | entrada `route` | `event,id,distance,directions[{lat,lng}]` | Valida e grava rota (upsert), calcula e publica frete |
| DeliveryStarted | entrada `route` | `event,route_id` | Recupera rota, publica cada ponto em ordem |
| FreightCalculated | saída `freight` | `event,route_id,amount` | Resultado de frete |
| DriverMoved | saída `simulation` | `event,route_id,lat,lng` | Posição simulada |

O arquivo `events.json` contém dois objetos de exemplo concatenados — **não** é um documento JSON único válido. Envie os objetos separadamente ao tópico.

## Regras verificáveis

1. `RouteCreated` exige ID não vazio, distância não negativa, uma ou mais coordenadas e latitudes/longitudes válidas.
2. `FreightService.Calculate(1)` retorna `0.45`; `Calculate(120)` retorna `18.30` com arredondamento matemático ao centavo.
3. MongoDB aceita somente operações com contexto e deadline; a URI vem de `MONGO_URI` obrigatória.
4. Publicação Kafka falha => `HandleEvent` falha => offset não confirmado. O consumidor encerra para que o evento seja reprocessado.
5. Direções são publicadas com intervalo de um segundo, sem goroutines que possam ficar bloqueadas.

## Limites de confiabilidade

- Entrega pelo menos uma vez, sujeita a duplicatas. A DLQ recebe eventos permanentemente inválidos e exige confirmação Kafka antes do commit da origem. Outbox transacional, deduplicação por ID e idempotência do produtor seguem pendentes.
- Kafka e Mongo não foram levantados juntos nesta entrega; o workflow testa apenas domínio, compilação e vet.
- O consumo sequencial limita throughput; uma simulação extensa demora `N-1` segundos apenas nas pausas.
- `compose.yaml` importa stacks de diretórios irmãos, exigindo workspace completo.

## Limites e compatibilidade v1.2.0

| Contrato | Limite | Comportamento de erro |
| --- | --- | --- |
| JSON de entrada | 256 KiB | Recusado antes de decodificar JSON |
| Pontos por rota | 1–600 | RouteCreated falha antes de persistir |
| Pontos já persistidos | 1–600, com latitudes/longitudes válidas | DeliveryStarted falha antes de publicar a sequência |
| Contexto cancelado | Não processar o evento | Erro propagado sem confirmar offset |

Como o simulador aguarda um segundo entre publicações consecutivas, 600 pontos equivalem a **599 segundos apenas em pausas**. Soma-se a latência de Kafka. A configuração de limites não é um mecanismo de deduplicação nem garante progresso quando uma mensagem inválida permanecer no tópico. Os produtores NestJS e outros clientes precisam manter o mesmo orçamento antes do envio ou tratar recusas.
