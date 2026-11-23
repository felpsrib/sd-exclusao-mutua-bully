#!/usr/bin/env bash
# Abre a apresentação em UMA janela de terminal, dividida em 6 painéis (tmux):
# log_server, P5 (coordenador inicial), P4, P3, P2 e P1, cada um em seu painel.
#
# Uso:  bash apresentacao_tmux.sh        (requer tmux; no Windows, use o WSL)
# Navegar entre painéis: Ctrl+b e as setas.  Sair de tudo: Ctrl+b e depois :kill-session
set -e
cd "$(dirname "$0")"
SESSAO=sd
PY=${PYTHON:-python}
[ -d .venv ] && ATIVA="source .venv/bin/activate; " || ATIVA=""

tmux kill-session -t $SESSAO 2>/dev/null || true
tmux new-session -d -s $SESSAO -x 220 -y 60 "${ATIVA}$PY log_server.py; read"
tmux split-window -h -t $SESSAO "${ATIVA}sleep 0.5; $PY processo.py 5; read"
tmux split-window -v -t $SESSAO:0.0 "${ATIVA}sleep 1.0; $PY processo.py 4; read"
tmux split-window -v -t $SESSAO:0.1 "${ATIVA}sleep 1.2; $PY processo.py 3; read"
tmux split-window -v -t $SESSAO:0.2 "${ATIVA}sleep 1.4; $PY processo.py 2; read"
tmux split-window -v -t $SESSAO:0.4 "${ATIVA}sleep 1.6; $PY processo.py 1; read"
tmux select-layout -t $SESSAO tiled
tmux set -t $SESSAO pane-border-status top
tmux attach -t $SESSAO
