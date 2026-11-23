"""
config.py
=========

Configuração central do trabalho: topologia, protocolo de mensagens e tempos.

Este arquivo está PRONTO. Você pode ajustar os tempos para experimentar, mas
não precisa alterar nada para cumprir o trabalho.

-----------------------------------------------------------------------------
1. TOPOLOGIA
-----------------------------------------------------------------------------
N processos, cada um identificado por um inteiro e escutando em uma porta TCP
de localhost. O servidor de log (gRPC) roda em outra porta.

    P1 -> 50051    P2 -> 50052    P3 -> 50053    P4 -> 50054    P5 -> 50055
    LogServer (gRPC) -> 60051

O coordenador inicial é o processo de MAIOR ID (P5), exatamente quem o
Algoritmo do Valentão elegeria se todos estivessem vivos.

-----------------------------------------------------------------------------
2. PROTOCOLO TEXTUAL (sockets TCP)
-----------------------------------------------------------------------------
Cada mensagem é UMA linha de texto terminada em "\\n":

    <TIPO>|<ID_ORIGEM>|<EXTRA>\\n        ex.: "REQUEST|3|\\n"

O campo EXTRA é opcional (fica vazio, mas o último "|" permanece).

Cada mensagem viaja em uma conexão TCP própria: abre, envia, (opcionalmente)
lê UMA linha de resposta, fecha. Há dois estilos de troca:

  * ASSÍNCRONA (sem resposta na mesma conexão)  -> processo._enviar(...)
  * SÍNCRONA   (pergunta e resposta)            -> processo._enviar_e_aguardar(...)

  Tipo         Estilo     Quem envia -> quem recebe       Significado
  -----------  ---------  ------------------------------  -----------------------------
  REQUEST      assíncr.   cliente -> coordenador          "quero entrar na RC"
  GRANT        assíncr.   coordenador -> cliente          "pode entrar"
  RELEASE      assíncr.   cliente -> coordenador          "saí da RC"
  ELECTION     síncrona   Pi -> Pj (j > i)                "estou fazendo eleição"
  ANSWER       (resposta) Pj -> Pi                        "estou vivo e sou maior: deixa comigo"
  COORDINATOR  síncrona   vencedor -> todos               "eu sou o novo coordenador"
  ACK          (resposta) todos -> vencedor               "ok"; EXTRA=NA_RC se o
                                                          processo está DENTRO da RC
  PING         síncrona   cliente -> coordenador          "você está vivo?"
  PONG         (resposta) coordenador -> cliente          "estou"

Por que GRANT é assíncrono? Porque o coordenador pode demorar MUITO para
conceder (a RC pode estar ocupada). Segurar a conexão do REQUEST aberta
durante esse tempo seria frágil; em vez disso o coordenador abre uma
conexão nova quando chegar a vez do cliente.

-----------------------------------------------------------------------------
3. TEMPOS
-----------------------------------------------------------------------------
Os tempos ficam no fim do arquivo. O processo os lê como ``config.NOME`` em
tempo de execução (e não com ``from config import NOME``), de modo que os
testes e as demonstrações podem acelerá-los sem editar este arquivo.
"""

from __future__ import annotations

from typing import Dict, NamedTuple

# =============================================================================
# 1. Topologia
# =============================================================================

#: Mapa  ID do processo -> porta TCP em localhost.
#: Para mudar o número de processos, basta editar este dicionário.
PORTAS: Dict[int, int] = {
    1: 50051,
    2: 50052,
    3: 50053,
    4: 50054,
    5: 50055,
}

HOST = "127.0.0.1"

#: Endereço do servidor gRPC de log.
LOG_SERVER_HOST = "127.0.0.1"
LOG_SERVER_PORTA = 60051

#: Coordenador inicial: o maior ID (quem o Valentão elegeria com todos vivos).
COORDENADOR_INICIAL = max(PORTAS)


# =============================================================================
# 2. Protocolo
# =============================================================================

# --- Exclusão mútua ---
REQUEST = "REQUEST"
GRANT = "GRANT"
RELEASE = "RELEASE"

# --- Eleição (Algoritmo do Valentão / Bully) ---
ELECTION = "ELECTION"
ANSWER = "ANSWER"
COORDINATOR = "COORDINATOR"
ACK = "ACK"

# --- Detecção de falhas (heartbeat) ---
PING = "PING"
PONG = "PONG"

#: Valor do campo EXTRA no ACK de quem está dentro da região crítica.
NA_RC = "NA_RC"

TIPOS_VALIDOS = frozenset(
    {REQUEST, GRANT, RELEASE, ELECTION, ANSWER, COORDINATOR, ACK, PING, PONG}
)


class Mensagem(NamedTuple):
    """Uma mensagem já decodificada.

    É uma tupla com nomes: ``msg.tipo`` é o mesmo que ``msg[0]``.
    """

    tipo: str
    origem: int
    extra: str = ""


def montar(tipo: str, origem: int, extra: str = "") -> bytes:
    """Monta uma mensagem do protocolo pronta para ``socket.sendall``.

    >>> montar("REQUEST", 3)
    b'REQUEST|3|\\n'
    """
    if "\n" in extra:
        raise ValueError("EXTRA não pode conter quebra de linha")
    return f"{tipo}|{origem}|{extra}\n".encode("utf-8")


def parsear(linha: str) -> Mensagem:
    """Decodifica uma linha do protocolo.

    Levanta ``ValueError`` se a linha estiver malformada: menos de três
    campos, tipo vazio ou origem que não é um inteiro. Pipes adicionais
    pertencem ao EXTRA (``"A|1|x|y"`` -> extra ``"x|y"``).

    >>> parsear("GRANT|5|\\n")
    Mensagem(tipo='GRANT', origem=5, extra='')
    """
    linha = linha.strip()
    partes = linha.split("|", 2)
    if len(partes) < 3:
        raise ValueError(f"mensagem malformada (faltam campos): {linha!r}")
    tipo, origem_txt, extra = partes
    if not tipo:
        raise ValueError(f"mensagem malformada (tipo vazio): {linha!r}")
    try:
        origem = int(origem_txt)
    except ValueError:
        raise ValueError(f"mensagem malformada (origem não numérica): {linha!r}") from None
    return Mensagem(tipo, origem, extra)


# =============================================================================
# 3. Tempos (segundos)
# =============================================================================
#
#   Linha do tempo de uma detecção de falha típica:
#
#   coord cai     próximo PING        PING falha          eleição termina
#      |------------- até INTERVALO_HEARTBEAT ---|-- TIMEOUT_RESPOSTA --|-- ~TIMEOUT_ELEICAO --|
#

#: Espera máxima por uma resposta ponto a ponto (PONG, ACK).
TIMEOUT_RESPOSTA = 2.0

#: Espera máxima por um ANSWER depois de enviar ELECTION a um processo.
#: Quem recebeu ANSWER espera até 2 x este valor pelo COORDINATOR.
TIMEOUT_ELEICAO = 3.0

#: Quanto um cliente espera pelo GRANT antes de desconfiar do coordenador.
#: Deve ser maior que (tamanho da fila x HOLD_RC), senão clientes desistem à
#: toa. Note que uma QUEDA do coordenador não depende deste valor: o cliente
#: percebe a troca de coordenador e refaz o pedido na hora.
TIMEOUT_GRANT = 60.0

#: Intervalo entre PINGs de cada cliente ao coordenador.
INTERVALO_HEARTBEAT = 2.0

#: Cada processo espera um tempo aleatório em [0, JITTER_INICIAL_MAX] antes do
#: primeiro PING. Sem isso, processos iniciados juntos pingariam no mesmo
#: instante e detectariam a falha todos ao mesmo tempo, o que esconde a
#: natureza assíncrona do sistema.
JITTER_INICIAL_MAX = 4.0

#: Tempo que um processo passa DENTRO da região crítica (simula trabalho).
#: 8 s dão folga para alternar entre terminais e ver a fila se formar.
HOLD_RC = 8.0
