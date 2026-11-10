# Requisitos

## Objetivo
Simulador de rotas e eventos de frete em Go, usando Kafka e MongoDB em ambiente Docker Compose.

RF01: executar o fluxo principal do domínio de Rota.

RF02: validar as entradas e devolver falhas previsíveis.

RF03: isolar conexões de infraestrutura.

RNF01: execução repetível via ferramentas do projeto.

RNF02: testes automatizados e documentação versionada.

RNF03: credenciais externas fora do controle de versão.

**Pendência:** O Compose referencia pastas irmãs ../mongodb e ../kafka; execute no workspace completo, não em clone isolado.

Requisitos extraídos do código e complementados por hipóteses didáticas identificadas.
