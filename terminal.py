"""
terminal.py
===========

Utilitários de saída colorida no terminal (PRONTO, não precisa mexer).

As cores ajudam a acompanhar a apresentação: cada processo tem uma cor fixa
(a mesma em todos os terminais) e cada tipo de evento tem um destaque.

As cores são desligadas automaticamente quando a saída não é um terminal
(ex.: redirecionada para arquivo ou capturada pelo pytest) ou quando a
variável de ambiente NO_COLOR está definida. Para forçar: SD_CORES=1 ou 0.
"""

from __future__ import annotations

import os
import sys

_RESET = "\033[0m"

#: Códigos ANSI por nome.
_CODIGOS = {
    "negrito": "1",
    "fraco": "2",
    "vermelho": "31",
    "verde": "32",
    "amarelo": "33",
    "azul": "34",
    "magenta": "35",
    "ciano": "36",
    "cinza": "90",
    "vermelho_forte": "1;97;41",
    "verde_forte": "1;30;42",
    "amarelo_forte": "1;30;43",
    "magenta_forte": "1;97;45",
}

#: Cor fixa de cada processo (por ID).
COR_PROCESSO = {1: "azul", 2: "verde", 3: "amarelo", 4: "magenta", 5: "ciano"}

#: Destaque por tipo de mensagem do protocolo.
COR_MENSAGEM = {
    "REQUEST": "azul",
    "GRANT": "verde",
    "RELEASE": "ciano",
    "ELECTION": "amarelo",
    "ANSWER": "amarelo",
    "COORDINATOR": "magenta",
    "ACK": "magenta",
    "PING": "cinza",
    "PONG": "cinza",
}


def _detectar() -> bool:
    forcado = os.environ.get("SD_CORES")
    if forcado is not None:
        return forcado not in ("0", "", "nao", "não", "false")
    if os.environ.get("NO_COLOR"):
        return False
    if not hasattr(sys.stdout, "isatty") or not sys.stdout.isatty():
        return False
    if os.name == "nt":  # habilita sequências ANSI no console do Windows 10+
        os.system("")
    return True


CORES_ATIVAS = _detectar()


def ativar_cores(ativo: bool) -> None:
    """Liga/desliga as cores (usado pela opção --sem-cores)."""
    global CORES_ATIVAS
    CORES_ATIVAS = ativo


def cor(texto: str, nome: str | None) -> str:
    """Envolve ``texto`` com o código ANSI da cor ``nome`` (se ativas)."""
    if not CORES_ATIVAS or not nome:
        return texto
    return f"\033[{_CODIGOS[nome]}m{texto}{_RESET}"


def cor_processo(pid: int) -> str:
    return COR_PROCESSO.get(pid, "negrito")


def rotulo_processo(pid: int) -> str:
    """'P3' na cor do processo 3."""
    return cor(f"P{pid}", cor_processo(pid))


def rotulo_mensagem(tipo: str) -> str:
    """'GRANT' na cor associada ao tipo."""
    return cor(tipo, COR_MENSAGEM.get(tipo))
