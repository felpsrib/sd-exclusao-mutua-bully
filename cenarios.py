"""
cenarios.py
===========

Executa, de forma AUTOMÁTICA e REPRODUTÍVEL, os cenários do trabalho em um
único programa (cada processo em suas próprias threads), confere o resultado
e mostra o que aconteceu:

  * o log de cada processo (com --log), como nos terminais da apresentação;
  * um diagrama de sequência no terminal (uma raia por processo);
  * uma página HTML em ``saida/<cenario>.html`` com a linha do tempo da RC,
    o diagrama de sequência completo e a tabela de acessos.

Cenários disponíveis:

  fila        Cenário 1: P5 coordena; P1..P4 pedem a RC quase juntos. A fila
              serializa os acessos na ordem de chegada.
  falha       Cenário 2: P5 cai com um cliente DENTRO da RC e outros na fila.
              O heartbeat detecta, o Valentão elege P4, e os clientes da fila
              refazem o pedido ao novo coordenador.
  retorno     P5 volta depois da falha e, pelo Valentão, retoma a coordenação.
  simultanea  Três processos detectam a falha e iniciam eleições ao mesmo
              tempo; ainda assim um único coordenador é eleito.
  todos       Executa todos, em sequência.

Uso:
    python cenarios.py fila
    python cenarios.py falha --log            # mostra também os logs dos processos
    python cenarios.py todos --hold 0.5       # acelera o tempo dentro da RC

Também é um ótimo teste de integração: o script termina com código 1 se
alguma verificação falhar.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import sys
import threading
import time
from typing import Callable, Dict, List

import config
import terminal as T
from log_server import LogServiceServicer, criar_servidor
from processo import Processo
from visualizacao import Rastreador, diagrama_texto, gerar_html

# =============================================================================
# Infraestrutura de cenário
# =============================================================================


class Cenario:
    """Ambiente de um cenário: log-server, processos e rastreador."""

    def __init__(self, nome: str, mostrar_log: bool) -> None:
        self.nome = nome
        self.mostrar_log = mostrar_log
        self.rastro = Rastreador().instalar()
        self.arbitro = LogServiceServicer(imprimir=False)
        self.servidor = criar_servidor(config.LOG_SERVER_PORTA, self.arbitro)
        self.servidor.start()
        self.procs: Dict[int, Processo] = {}
        self.falhas: List[str] = []
        self.t0 = time.time()

    # --- processos ---------------------------------------------------
    def subir(self, *ids: int, coord: int = 5, intervalo: float = 0.15) -> None:
        for pid in ids:
            p = Processo(pid, coord)
            p.iniciar()
            self.procs[pid] = p
            time.sleep(intervalo)

    def derrubar(self, pid: int) -> None:
        self.procs[pid].parar()

    def pedir(self, pid: int, mensagem: str) -> threading.Thread:
        t = threading.Thread(target=self.procs[pid].requisitar_rc, args=(mensagem,), daemon=True)
        t.start()
        return t

    # --- narração ----------------------------------------------------
    def fase(self, texto: str) -> None:
        self.rastro.marcar(texto)
        print(T.cor(f"\n[+{time.time() - self.t0:6.2f}s] ── {texto} ──", "negrito"), flush=True)

    def verificar(self, condicao: bool, descricao: str) -> None:
        marca = T.cor("✔", "verde") if condicao else T.cor("✖", "vermelho")
        print(f"   {marca} {descricao}", flush=True)
        if not condicao:
            self.falhas.append(descricao)

    def esperar(self, condicao: Callable[[], bool], timeout: float) -> bool:
        limite = time.time() + timeout
        while time.time() < limite:
            if condicao():
                return True
            time.sleep(0.05)
        return condicao()

    def coordenadores_vistos(self) -> Dict[int, int]:
        return {pid: p.coordenador_id for pid, p in self.procs.items() if p.ativo}

    # --- encerramento ------------------------------------------------
    def encerrar(self, titulo: str, descricao: str) -> bool:
        Processo.silencioso = True
        for p in self.procs.values():
            if p.ativo:
                p.parar()
        Processo.silencioso = False
        self.servidor.stop(grace=0.2)
        self.rastro.remover()
        self.verificar(not self.arbitro.violacoes,
                       "árbitro (log-server): nenhuma violação de exclusão mútua")

        eventos = self.rastro.copia()
        print(T.cor("\nDiagrama de sequência (PING/PONG omitidos):\n", "negrito"))
        print(diagrama_texto(eventos))
        os.makedirs("saida", exist_ok=True)
        caminho = gerar_html(eventos, os.path.join("saida", f"{self.nome}.html"), titulo, descricao)
        print(f"\nVisualização completa: {T.cor(os.path.abspath(caminho), 'ciano')}")
        if self.falhas:
            print(T.cor(f"\n{len(self.falhas)} verificação(ões) falharam no cenário '{self.nome}'.", "vermelho"))
        else:
            print(T.cor(f"\nCenário '{self.nome}': todas as verificações passaram.", "verde"))
        time.sleep(0.5)  # libera as portas para o próximo cenário
        return not self.falhas


@contextlib.contextmanager
def _logs(mostrar: bool):
    """Esconde o log dos processos quando --log não foi pedido."""
    Processo.silencioso = not mostrar
    try:
        yield
    finally:
        Processo.silencioso = False


# =============================================================================
# Cenários
# =============================================================================


def cenario_fila(mostrar_log: bool) -> bool:
    c = Cenario("fila", mostrar_log)
    with _logs(mostrar_log):
        c.fase("Sobem P5 (coordenador) e os clientes P1..P4")
        c.subir(5, 4, 3, 2, 1)

        c.fase("P1, P2, P3 e P4 pedem a RC quase ao mesmo tempo")
        threads = []
        for pid in (1, 2, 3, 4):
            threads.append(c.pedir(pid, f"operacao-de-P{pid}"))
            time.sleep(0.05)
        for t in threads:
            t.join(timeout=4 * config.HOLD_RC + 15)

        c.fase("Conferência")
        ordem = [a["process_id"] for a in c.arbitro.acessos]
        c.verificar(len(ordem) == 4, f"4 acessos registrados (vieram {len(ordem)})")
        c.verificar(ordem == [1, 2, 3, 4], f"ordem FIFO de chegada: {['P%d' % p for p in ordem]}")
        c.verificar(all(a["coordenador_id"] == 5 for a in c.arbitro.acessos),
                    "todos autorizados por P5")
    return c.encerrar(
        "Cenário 1 — fila sem falhas",
        "P5 coordena; P1..P4 pedem a RC quase juntos. O coordenador concede a um "
        "e enfileira os demais; cada RELEASE libera o próximo da fila.")


def cenario_falha(mostrar_log: bool) -> bool:
    c = Cenario("falha", mostrar_log)
    with _logs(mostrar_log):
        c.fase("Sobem P5 (coordenador) e os clientes P1..P4")
        c.subir(5, 4, 3, 2, 1)

        c.fase("P2 acessa a RC normalmente, autorizado por P5")
        c.pedir(2, "antes-da-falha").join(timeout=config.HOLD_RC + 10)

        c.fase("P1 entra na RC; P3 e P4 ficam na fila de P5")
        threads = [c.pedir(1, "durante-a-falha")]
        time.sleep(0.2)
        threads += [c.pedir(3, "na-fila-de-P5"), c.pedir(4, "na-fila-de-P5")]
        time.sleep(0.3)

        c.fase("P5 FALHA (com P1 dentro da RC e P3, P4 na fila)")
        c.derrubar(5)
        t_falha = time.time()

        eleito = c.esperar(lambda: set(c.coordenadores_vistos().values()) == {4},
                           timeout=config.INTERVALO_HEARTBEAT + 4 * config.TIMEOUT_ELEICAO + 5)
        c.fase(f"Heartbeat detectou e o Valentão elegeu P4 ({time.time() - t_falha:.1f}s após a falha)")
        c.verificar(eleito, f"todos concordam que P4 é o coordenador: {c.coordenadores_vistos()}")

        for t in threads:
            t.join(timeout=4 * config.HOLD_RC + 20)
        c.fase("P2 volta a pedir a RC, agora a P4")
        c.pedir(2, "apos-a-falha").join(timeout=config.HOLD_RC + 10)

        c.fase("Conferência")
        acessos = c.arbitro.acessos
        c.verificar(len(acessos) == 5, f"5 acessos registrados (vieram {len(acessos)})")
        por_p4 = sorted(a["process_id"] for a in acessos if a["coordenador_id"] == 4)
        c.verificar(por_p4 == [2, 3, 4], f"P3, P4 e P2 atendidos por P4: {['P%d' % p for p in por_p4]}")
        entradas_p4 = [e["t"] for e in c.rastro.copia()
                       if e["ev"] == "rc_entrada" and e.get("coord") == 4]
        atraso = min(entradas_p4) - t_falha if entradas_p4 else float("inf")
        c.verificar(atraso < config.TIMEOUT_GRANT,
                    f"a fila andou {atraso:.1f}s após a falha (sem esperar TIMEOUT_GRANT={config.TIMEOUT_GRANT:.0f}s)")
    return c.encerrar(
        "Cenário 2 — falha do coordenador",
        "P5 cai enquanto P1 está na RC e P3, P4 estão na fila. O heartbeat detecta a "
        "queda, o Algoritmo do Valentão elege P4 e os clientes refazem o pedido ao novo "
        "coordenador. P1, que estava na RC, é reconhecido por P4 (ACK com NA_RC).")


def cenario_retorno(mostrar_log: bool) -> bool:
    c = Cenario("retorno", mostrar_log)
    with _logs(mostrar_log):
        c.fase("Sobem P5 (coordenador), P4, P3 e P2")
        c.subir(5, 4, 3, 2)
        c.fase("P5 falha")
        c.derrubar(5)
        c.esperar(lambda: set(c.coordenadores_vistos().values()) == {4}, timeout=20)
        c.verificar(set(c.coordenadores_vistos().values()) == {4}, "P4 eleito na ausência de P5")
        c.pedir(3, "sob-P4").join(timeout=config.HOLD_RC + 10)

        c.fase("P5 volta: ao subir, faz eleição e retoma a coordenação")
        c.procs[5] = Processo(5)
        c.procs[5].iniciar()
        ok = c.esperar(lambda: set(c.coordenadores_vistos().values()) == {5}, timeout=10)
        c.verificar(ok, f"todos voltam a ver P5 como coordenador: {c.coordenadores_vistos()}")

        c.fase("P2 pede a RC; quem autoriza é P5")
        c.pedir(2, "apos-o-retorno").join(timeout=config.HOLD_RC + 10)
        c.verificar(c.arbitro.acessos[-1]["coordenador_id"] == 5, "último acesso autorizado por P5")
    return c.encerrar(
        "Cenário extra — o antigo coordenador volta",
        "Pelo Valentão, quem (re)entra no sistema faz uma eleição. P5, o maior ID, "
        "vence e retoma a coordenação; sem isso haveria dois coordenadores.")


def cenario_simultanea(mostrar_log: bool) -> bool:
    c = Cenario("simultanea", mostrar_log)
    with _logs(mostrar_log):
        c.fase("Sobem P5 (coordenador) e P1..P4")
        c.subir(5, 4, 3, 2, 1)
        c.fase("P5 falha; P1, P2 e P3 iniciam eleições ao mesmo tempo")
        c.derrubar(5)
        ts = [threading.Thread(target=c.procs[p].iniciar_eleicao) for p in (1, 2, 3)]
        for t in ts:
            t.start()
        for t in ts:
            t.join(timeout=30)
        ok = c.esperar(lambda: set(c.coordenadores_vistos().values()) == {4}, timeout=10)
        c.verificar(ok, f"um único coordenador (P4), visto por todos: {c.coordenadores_vistos()}")
    return c.encerrar(
        "Cenário extra — eleições simultâneas",
        "Várias eleições disparadas ao mesmo tempo convergem para um único vencedor: "
        "o maior ID vivo.")


CENARIOS = {
    "fila": cenario_fila,
    "falha": cenario_falha,
    "retorno": cenario_retorno,
    "simultanea": cenario_simultanea,
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Executa e visualiza os cenários do trabalho")
    parser.add_argument("cenario", choices=[*CENARIOS, "todos"])
    parser.add_argument("--log", action="store_true", help="mostra o log de cada processo")
    parser.add_argument("--hold", type=float, default=1.5,
                        help="segundos dentro da RC (padrão 1.5; config.py usa 8 na apresentação)")
    parser.add_argument("--sem-cores", action="store_true")
    args = parser.parse_args()

    if args.sem_cores:
        T.ativar_cores(False)
    config.HOLD_RC = args.hold
    config.JITTER_INICIAL_MAX = 1.0
    config.INTERVALO_HEARTBEAT = 1.0
    config.TIMEOUT_ELEICAO = 1.5

    nomes = list(CENARIOS) if args.cenario == "todos" else [args.cenario]
    resultados = {}
    for nome in nomes:
        print(T.cor(f"\n{'=' * 78}\n CENÁRIO: {nome}\n{'=' * 78}", "negrito"))
        resultados[nome] = CENARIOS[nome](args.log)
    if len(nomes) > 1:
        print("\nResumo: " + ", ".join(f"{n}={'ok' if r else 'FALHOU'}" for n, r in resultados.items()))
    return 0 if all(resultados.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
