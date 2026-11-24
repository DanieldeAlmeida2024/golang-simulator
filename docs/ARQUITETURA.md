# Arquitetura — Go Simulator

Versão 1.3.0 — 24/11/2026

## Processamento de um evento

`cmd/simulator/main.go` inicializa MongoDB, o consumidor Kafka e o tratamento de SIGTERM. Frete, simulação e DLQ compartilham a configuração de publicação síncrona com `RequireAll`, que aguarda as réplicas em sincronia. O consumidor confirma a origem após publicar os eventos derivados ou após publicar na DLQ um evento permanentemente inválido.

`internal/eventhub.go` publica cada resultado com deadline de 10 segundos. Publicação malsucedida vira erro propagado ao consumidor, encerrando sem commit. Para `DeliveryStarted`, cada coordenada gera um `DriverMoved`, em sequência, com pausa de um segundo entre publicações. Não usa canal sem consumidor ou temporizador de 500 ms.

`internal/handler.go` valida o evento de rota (ID, distância, direções, coordenadas) e transforma direções em movimentos na mesma ordem.

`internal/route.go` faz upsert e leitura MongoDB usando `context.WithTimeout(ctx,8s)` em vez de contexto nulo. Evita registrar coordenadas completas no log.

## Contratos

- Entrada topic `route`: `RouteCreated {event,id,distance,directions[]}` e `DeliveryStarted {event,route_id}`.
- Saída topic `freight`: `FreightCalculated {event,route_id,amount}`.
- Saída topic `simulation`: `DriverMoved {event,route_id,lat,lng}`.
- Frete: `math.Round((distance*0.15 + 0.30)*100)/100`, arredondado em centavos. Corrige perda de centavo pela representação binária de ponto flutuante.

## Garantias e riscos

O offset **não é confirmado** quando banco ou escritor falha. Isso reduz perda silenciosa, mas não fornece outbox transacional nem exatamente-uma-vez. Uma falha depois de publicar um evento derivado, mas antes de confirmar o original, pode gerar duplicidade ao reiniciar.

O consumo serial fica ocupado enquanto uma rota longa é simulada. Falhas transitórias e falhas de envio para a DLQ encerram o consumo sem confirmar a mensagem de origem. A política de reinício depende do supervisor externo. Vazão, validação com broker real, reprocessamento com backoff no consumidor e idempotência permanecem pendentes. O Compose usa recursos de pastas irmãs.
