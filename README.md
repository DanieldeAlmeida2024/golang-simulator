# golang-simulator

Simulador de rotas e eventos de frete em Go, usando Kafka e MongoDB em ambiente Docker Compose.

## Requisitos

Go 1.23.1, Kafka, MongoDB e Docker Compose. O Compose referencia pastas irmãs ../mongodb e ../kafka; execute no workspace completo, não em clone isolado.

## Execução

```bash
docker compose up -d
docker compose exec simulator sh
go run cmd/simulator/main.go
```

## Guias

- [Arquitetura](docs/ARQUITETURA.md)
- [Requisitos](docs/REQUISITOS.md)
- [Diagrama UML](docs/UML.puml)
- [Plano de testes](docs/PLANO_TESTES.md)
- [Guia para IA](AGENTS.md)

## Documentação original preservada

# Imersão Fullcycle 20 - Sistema de rastreamento de veículos

## Descrição

Repositório do Golang (simulador de trajeto e cálculo de frete).

## Rodar a aplicação

Levante os containers:

```
docker compose up -d
```

Entre no container simulator:

```
docker compose exec simulator bash
```

Rode o comando para executar o projeto:

```
go run cmd/simulator/main.go
```

## Documentação técnica específica

- [Projeto e eventos](docs/projeto/ESPECIFICACAO.md)
- [UML](docs/uml/README.md)
- [Testes](docs/testes/PLANO_DE_TESTES.md)
- [Versão](docs/controle/VERSIONAMENTO.md)

## Documentação técnica específica

[Projeto](docs/projeto/ESPECIFICACAO.md) · [UML](docs/uml/README.md) · [Testes](docs/testes/PLANO_DE_TESTES.md) · [Versionamento](docs/controle/VERSIONAMENTO.md)
