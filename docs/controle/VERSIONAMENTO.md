# Histórico documental — Go Simulator

## GO-SIM 1.1.0 · 14/11/2026

- Corrigidos commit prematuro de offsets, perda de erros de publicação e o encerramento do listener após 500 ms.
- Simulação de deslocamento passou a ser sequencial, com um segundo entre pontos.
- MongoDB agora usa contexto e timeout; URI obrigatória no ambiente.
- Adicionados testes Go de domínio/contrato e workflow de compilação e vet.
- Corrigido arredondamento de frete e removido log detalhado de rota com coordenadas.
- **Pendências:** Kafka/Mongo E2E, deduplicação, DLQ e robustez operacional.

## GO-SIM 1.0.0 · 11/11/2026

Inventário e análise estática original, referência `bb46a6831ff6c5586d69c1113c52e9dd6b5348a2`. As limitações daquela versão foram documentadas nos arquivos históricos.

[Projeto](../projeto/ESPECIFICACAO.md) · [UML](../uml/README.md) · [Testes](../testes/PLANO_DE_TESTES.md).

## GO-SIM 1.2.0 · 14/11/2026

- Orçamento de 256 KiB por evento Kafka antes do parse JSON.
- Máximo de 600 coordenadas para rotas novas e recuperadas do MongoDB.
- Validação de coordenadas persistidas antes de publicar o primeiro DriverMoved.
- Rejeição imediata quando o contexto está cancelado.
- Testes unitários GO-U10 a GO-U15 e documentação dos limites.

**Compatibilidade:** rotas ou eventos que excedam os novos limites passam a falhar explicitamente; produtores do sistema de rastreamento devem alinhar tamanho e quantidade de pontos. Continua sendo um simulador de laboratório com semântica de entrega pelo menos uma vez; integrações reais ainda requerem testes dedicados.
