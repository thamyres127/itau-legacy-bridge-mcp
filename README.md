# Itaú Legacy Bridge MCP

Projeto de portfólio que demonstra uma arquitetura de integração e monitoramento de eventos financeiros usando conceitos de **Kafka, Event Store, Consumer Groups e MCP**.

🔗 **Demonstração ao vivo:** https://thamyres127.github.io/itau-legacy-bridge-mcp/

## Problema que resolve

Em sistemas financeiros, falhas na validação de eventos e atrasos no processamento podem causar inconsistências entre serviços.

O projeto demonstra como identificar eventos inválidos, monitorar o atraso dos consumidores (*consumer lag*) e simular a recuperação de mensagens.

## Regras de negócio

O sistema trabalha com três tipos de eventos:

- **TED:** valida os dados da transferência e o valor.
- **Boleto:** verifica os dados e o valor do pagamento.
- **Débito automático:** valida as informações necessárias para o processamento.

Eventos inválidos são identificados antes de serem considerados processados.

## Arquitetura

```text
Producer
   ↓
Kafka (simulado)
   ↓
Validação de contratos
   ↓
Consumer Groups
   ↓
Monitoramento e diagnóstico MCP
   ↓
Replay de eventos
```

## Tecnologias e dependências

| Tecnologia | Utilização |
|---|---|
| Java | Simulação de eventos e consumidores |
| Python | Servidor MCP e diagnóstico |
| SQLite | Simulação local do armazenamento |
| PostgreSQL / SQL | Estrutura do Event Store |
| HTML, CSS e JavaScript | Dashboard de monitoramento |

O projeto utiliza bibliotecas nativas de Java e Python (`json`, `datetime` e `sqlite3`), sem dependências externas nos exemplos.

## Funcionalidades

- Simulação de eventos financeiros.
- Validação de contratos.
- Monitoramento de Consumer Groups e offsets.
- Identificação de Consumer Lag.
- Diagnóstico de falhas.
- Simulação de replay de eventos.
- Dashboard de observabilidade.

## Estrutura do projeto

```text
itau-legacy-bridge-mcp/
├── index.html
├── sql/
│   └── event_store.sql
├── core-kafka-java/
│   └── kafkaoffsetbridge.java
└── mcp-server-python/
    └── server.py
```

## Observação

Este projeto é uma demonstração educacional. Kafka, consumidores e integração do dashboard são simulados. O servidor Python utiliza SQLite em memória, e o SQL apresenta a estrutura proposta para PostgreSQL. Não existe conexão com sistemas internos ou operações reais do Itaú.

## Objetivo

Demonstrar conhecimentos básicos de integração de sistemas, processamento de eventos, SQL, Java, Python e monitoramento de aplicações.
