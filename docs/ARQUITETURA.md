# Arquitetura

Simulador de rotas e eventos de frete em Go, usando Kafka e MongoDB em ambiente Docker Compose.

## Componentes
- Interface de entrada / consumidor.
- Casos de uso, serviços ou eventos.
- Entidades de domínio (Rota).
- Persistência / mensageria externa.

A implementação atual é a fonte da verdade; as camadas acima são apenas uma visão para navegação.

## Decisões
Separar domínio de integração externa. Usar testes unitários com adaptadores simulados e integração real apenas em ambiente preparado. Não migrar a tecnologia existente por preferência estética.

## Dependências
O Compose referencia pastas irmãs ../mongodb e ../kafka; execute no workspace completo, não em clone isolado.
