# Trabalho de Sistemas Distribuídos — Esqueleto

**Exclusão mútua com coordenador central + eleição pelo Algoritmo do Valentão (Bully).**
O enunciado completo está em `enunciado.pdf`.

## Estrutura

```
esqueleto/
├── processo.py            # <- AQUI VOCÊ IMPLEMENTA (procure por "TODO")
├── config.py              # topologia, protocolo de mensagens e tempos      (pronto)
├── log_service.proto      # contrato gRPC do servidor de log                 (pronto)
├── log_server.py          # servidor de log = árbitro da RC                  (pronto)
├── terminal.py            # cores no terminal                                (pronto)
├── visualizacao.py        # diagramas de sequência (terminal e HTML)         (pronto)
├── cenarios.py            # executa e desenha os cenários automaticamente   (pronto)
├── testes.py              # testes por níveis: sua especificação executável  (pronto)
├── conftest.py, pytest.ini
├── gen_proto.py / .sh     # gera log_service_pb2*.py a partir do .proto
├── apresentacao_tmux.sh   # abre os 6 terminais da apresentação de uma vez
└── requirements.txt
```

## Começando

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python gen_proto.py                # gera os stubs gRPC
python -m pytest -m nivel1         # deve passar já com o esqueleto
```

## Ordem de implementação

| Parte | Métodos em `processo.py` | Testes |
|---|---|---|
| A — coordenador | `_atender_request`, `_atender_release` | `pytest -m nivel2 -k "n2_0 or n2_10 or n2_11"` (sem rede) |
| B — cliente | `requisitar_rc`, `_receber_grant` | `pytest -m nivel2` |
| C — eleição | `_receber_election`, `_conduzir_eleicao`, `_tornar_coordenador`, `_receber_coordinator` | `pytest -m nivel3` |
| D — heartbeat | `_heartbeat` | `pytest -m "nivel3 or nivel4"` |
| Desafios | itens `[DESAFIO]` nos TODOs | `pytest -m desafio` |

Quando um teste falha, o pytest mostra o **diagrama de sequência** do que
aconteceu (seção *Captured stdout*). Leia-o como leria um log.

## Vendo o sistema funcionar

**Automático** (um comando, gera `saida/<cenario>.html`):

```bash
python cenarios.py fila            # Cenário 1
python cenarios.py falha --log     # Cenário 2, com o log de cada processo
python cenarios.py todos
```

**Manual** (como na apresentação), um terminal por processo:

```bash
python log_server.py               # terminal 1
python processo.py 5               # terminal 2 (coordenador inicial)
python processo.py 4               # ... e assim por diante até P1
```

ou, com tmux (Linux/macOS/WSL): `bash apresentacao_tmux.sh`.

Comandos dentro de um processo: `request [mensagem]`, `status`, `eleicao`, `sair`, `ajuda`.
Para simular a falha do coordenador, digite `sair` (ou Ctrl+C) no terminal dele.
