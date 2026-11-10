# Plano de testes

T01: cenário principal com dados válidos.
T02: entrada inválida e tratamento de erro.
T03: conexão externa indisponível.
T04: testes documentais e validação de UML.

```bash
docker compose up -d
docker compose exec simulator sh
go run cmd/simulator/main.go
python -m unittest discover -s tests -p test_documentacao.py -v
```

Os testes de documentação não verificam a execução do projeto. O Compose referencia pastas irmãs ../mongodb e ../kafka; execute no workspace completo, não em clone isolado.
