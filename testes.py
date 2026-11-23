"""
testes.py
=========

Suíte de testes que guia (e avalia) a implementação. Os testes estão
organizados em NÍVEIS, na ordem sugerida de implementação:

  Nível 1  Protocolo e infraestrutura .......... já passa com o esqueleto
  Nível 2  Exclusão mútua (Partes A e B) ....... coordenador + cliente
  Nível 3  Eleição e heartbeat (Partes C e D) .. Algoritmo do Valentão
  Nível 4  Falha e continuidade ................ o Cenário 2 completo
  Nível 5  Desafios (bônus) .................... situações de borda

Como rodar:

    python -m pytest testes.py -v                 # tudo
    python -m pytest testes.py -v -m nivel2       # só um nível
    python -m pytest testes.py -v -m "not desafio"   # sem os bônus
    python -m pytest testes.py -v -k fila         # testes com "fila" no nome
    python -m pytest testes.py -x                 # para no primeiro erro

Cada teste tem limite de tempo (ver pytest.ini): um deadlock vira falha, e
não um terminal travado.

Como os testes verificam a exclusão mútua
-----------------------------------------
Não basta conferir que "todo mundo entrou". Cada processo dos testes tem
seu ``_executar_rc`` envolvido por um MONITOR, que registra quantos
processos estão dentro da RC a cada instante. Se dois estiverem lá ao mesmo
tempo, o teste falha apontando quem. O servidor de log (árbitro) faz a
mesma verificação de forma independente, via gRPC.
"""

from __future__ import annotations

import threading
import time
from typing import Callable, Dict, Iterable, List, Optional

import pytest

import config

# ---------------------------------------------------------------------------
# Tempos acelerados para os testes. processo.py lê ``config.X`` em tempo de
# execução, então basta sobrescrever aqui (config.py continua com os valores
# da apresentação manual).
# ---------------------------------------------------------------------------
config.HOLD_RC = 0.2
config.JITTER_INICIAL_MAX = 0.0
config.INTERVALO_HEARTBEAT = 0.3
config.TIMEOUT_RESPOSTA = 1.0
config.TIMEOUT_ELEICAO = 1.0
config.TIMEOUT_GRANT = 10.0

from config import (  # noqa: E402
    ACK,
    ANSWER,
    COORDINATOR,
    ELECTION,
    GRANT,
    NA_RC,
    PING,
    PONG,
    RELEASE,
    REQUEST,
    TIPOS_VALIDOS,
    Mensagem,
    montar,
    parsear,
)
from log_server import LogServiceServicer, criar_servidor  # noqa: E402
from processo import Processo  # noqa: E402
from visualizacao import Rastreador, diagrama_texto  # noqa: E402


# =============================================================================
# Ferramentas dos testes
# =============================================================================


def esperar_ate(condicao: Callable[[], bool], timeout: float = 5.0, passo: float = 0.05) -> bool:
    """Espera até ``condicao()`` ser verdadeira (ou o tempo acabar)."""
    limite = time.monotonic() + timeout
    while time.monotonic() < limite:
        if condicao():
            return True
        time.sleep(passo)
    return condicao()


class MonitorRC:
    """Observa as entradas/saídas da RC e detecta sobreposições."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.dentro: List[int] = []
        self.ordem: List[int] = []          # ordem em que os processos entraram
        self.autorizadores: List[int] = []  # coordenador visto por quem entrou
        self.violacoes: List[str] = []

    def envolver(self, proc: Processo) -> None:
        original = proc._executar_rc  # noqa: SLF001

        def monitorado(mensagem: str) -> None:
            with self._lock:
                if self.dentro:
                    self.violacoes.append(
                        f"P{proc.meu_id} entrou com P{self.dentro} ainda dentro da RC")
                self.dentro.append(proc.meu_id)
                self.ordem.append(proc.meu_id)
                self.autorizadores.append(proc.coordenador_id)
            try:
                original(mensagem)
            finally:
                with self._lock:
                    self.dentro.remove(proc.meu_id)

        proc._executar_rc = monitorado  # noqa: SLF001

    def assert_exclusao(self) -> None:
        assert not self.violacoes, "EXCLUSÃO MÚTUA VIOLADA: " + "; ".join(self.violacoes)


class Cluster:
    """Sobe e derruba processos de teste, com monitor de RC."""

    def __init__(self, monitor: MonitorRC) -> None:
        self.monitor = monitor
        self.procs: Dict[int, Processo] = {}

    def subir(self, ids: Iterable[int], coord: Optional[int] = 5) -> Dict[int, Processo]:
        for pid in ids:
            p = Processo(pid, coordenador_inicial=coord)
            self.monitor.envolver(p)
            p.iniciar()
            self.procs[pid] = p
        # Espera a eleição inicial do coordenador presumido (se viva) terminar.
        esperar_ate(lambda: not any(p._em_eleicao for p in self.vivos()), timeout=3)  # noqa: SLF001
        time.sleep(0.1)
        return self.procs

    def derrubar(self, pid: int) -> None:
        self.procs[pid].parar()

    def vivos(self) -> List[Processo]:
        return [p for p in self.procs.values() if p.ativo]

    def coordenadores(self) -> Dict[int, int]:
        """Visão de cada processo vivo: {pid: coordenador que ele enxerga}."""
        return {p.meu_id: p.coordenador_id for p in self.vivos()}

    def todos_concordam_em(self, coord: int) -> bool:
        vis = self.coordenadores()
        return bool(vis) and set(vis.values()) == {coord}

    def pedir_em_paralelo(self, pids: Iterable[int], prefixo: str = "op",
                          atraso: float = 0.05) -> Dict[int, bool]:
        """Cada pid pede a RC em sua própria thread; retorna {pid: sucesso}."""
        resultados: Dict[int, bool] = {}
        threads = []
        for pid in pids:
            def alvo(p=self.procs[pid]):
                resultados[p.meu_id] = p.requisitar_rc(f"{prefixo}-P{p.meu_id}")
            t = threading.Thread(target=alvo)
            t.start()
            threads.append(t)
            time.sleep(atraso)
        for t in threads:
            t.join(timeout=config.TIMEOUT_GRANT * 2)
        return resultados

    def encerrar(self) -> None:
        for p in self.procs.values():
            try:
                p.parar()
            except Exception:
                pass
        time.sleep(0.3)  # deixa as portas livres para o próximo teste


@pytest.fixture()
def arbitro():
    """Servidor de log real (gRPC), em memória, que detecta violações."""
    servicer = LogServiceServicer(imprimir=False)
    servidor = criar_servidor(config.LOG_SERVER_PORTA, servicer)
    servidor.start()
    yield servicer
    servidor.stop(grace=0.2)


@pytest.fixture()
def rastro():
    r = Rastreador().instalar()
    yield r
    r.remover()


@pytest.fixture()
def cluster(arbitro, rastro, request):
    c = Cluster(MonitorRC())
    yield c
    c.encerrar()
    # Se o teste falhou, imprime o diagrama de sequência para ajudar a depurar
    # (aparece na seção "Captured stdout" do relatório do pytest).
    relatorio = getattr(request.node, "rep_call", None)
    if relatorio is not None and relatorio.failed:
        print("\n=== Diagrama de sequência do teste que falhou ===")
        print(diagrama_texto(rastro.copia(), ascii_puro=True))


def msgs(rastro: Rastreador, tipo: str, de: Optional[int] = None, para: Optional[int] = None,
         apos: float = 0.0) -> List[dict]:
    """Mensagens entregues de um tipo (filtros opcionais por origem/destino/instante)."""
    return [e for e in rastro.copia()
            if e["ev"] == "msg" and e["tipo"] == tipo and e.get("ok")
            and (de is None or e["de"] == de) and (para is None or e["para"] == para)
            and e["t"] >= apos]


def coordenador_isolado(pid: int = 5) -> tuple:
    """Um coordenador SEM rede, para testar a lógica da fila isoladamente.

    ``_enviar`` é substituído por um gravador: as mensagens que o coordenador
    tentaria enviar ficam em ``enviados`` como (destino, tipo).
    """
    p = Processo(pid, coordenador_inicial=pid)
    enviados: List[tuple] = []

    def gravar(destino: int, tipo: str, extra: str = "") -> bool:
        enviados.append((destino, tipo))
        return True

    p._enviar = gravar  # noqa: SLF001
    return p, enviados


# =============================================================================
# NÍVEL 1 -- Protocolo e infraestrutura (já passa com o esqueleto)
# =============================================================================

@pytest.mark.nivel1
@pytest.mark.parametrize("tipo", sorted(TIPOS_VALIDOS))
def test_n1_01_montar_e_parsear_ida_e_volta(tipo):
    """Toda mensagem do protocolo sobrevive a montar -> parsear."""
    msg = parsear(montar(tipo, 3, "x").decode("utf-8"))
    assert msg == Mensagem(tipo, 3, "x")
    assert msg.tipo == msg[0] == tipo


@pytest.mark.nivel1
@pytest.mark.parametrize("linha", ["lixo sem pipes", "", "REQUEST|", "REQUEST|abc|", "|3|"])
def test_n1_02_parsear_rejeita_malformadas(linha):
    with pytest.raises(ValueError):
        parsear(linha)


@pytest.mark.nivel1
def test_n1_03_extra_preserva_pipes():
    assert parsear("ACK|2|a|b|c\n") == Mensagem("ACK", 2, "a|b|c")


@pytest.mark.nivel1
def test_n1_04_ping_pong_pela_rede(cluster):
    """A infraestrutura de rede funciona: PING recebe PONG."""
    procs = cluster.subir([5, 2])
    resp = procs[2]._enviar_e_aguardar(5, PING)  # noqa: SLF001
    assert resp is not None, "P5 deveria ter respondido ao PING"
    assert resp.tipo == PONG and resp.origem == 5


@pytest.mark.nivel1
def test_n1_05_enviar_para_processo_inexistente_retorna_false(cluster):
    procs = cluster.subir([5, 2])
    assert procs[2]._enviar(3, REQUEST) is False  # P3 não está rodando  # noqa: SLF001
    assert procs[2]._enviar_e_aguardar(3, PING) is None  # noqa: SLF001


# =============================================================================
# NÍVEL 2 -- Exclusão mútua centralizada
# =============================================================================
# 2a) Lógica do coordenador, testada SEM rede (rápidos e determinísticos).


@pytest.mark.nivel2
def test_n2_06_request_com_rc_livre_concede_na_hora():
    p, enviados = coordenador_isolado()
    p._atender_request(1)  # noqa: SLF001
    assert p.titular_rc == 1
    assert p.fila_requests == []
    assert enviados == [(1, GRANT)]


@pytest.mark.nivel2
def test_n2_07_request_com_rc_ocupada_vai_para_o_fim_da_fila():
    p, enviados = coordenador_isolado()
    for pid in (1, 3, 2):
        p._atender_request(pid)  # noqa: SLF001
    assert p.titular_rc == 1
    assert p.fila_requests == [3, 2], "a fila deve ser FIFO (ordem de chegada)"
    assert enviados == [(1, GRANT)], "só o titular recebe GRANT"


@pytest.mark.nivel2
def test_n2_08_release_passa_a_rc_ao_proximo_da_fila():
    p, enviados = coordenador_isolado()
    for pid in (1, 3, 2):
        p._atender_request(pid)  # noqa: SLF001
    p._atender_release(1)  # noqa: SLF001
    assert p.titular_rc == 3
    assert p.fila_requests == [2]
    assert enviados[-1] == (3, GRANT)
    p._atender_release(3)  # noqa: SLF001
    p._atender_release(2)  # noqa: SLF001
    assert p.titular_rc is None and p.fila_requests == []
    assert [d for d, t in enviados if t == GRANT] == [1, 3, 2]


@pytest.mark.nivel2
def test_n2_09_release_de_quem_nao_e_titular_e_ignorado():
    """Um RELEASE atrasado/indevido NÃO pode liberar a RC de outro processo."""
    p, enviados = coordenador_isolado()
    p._atender_request(1)  # noqa: SLF001
    p._atender_request(2)  # noqa: SLF001
    p._atender_release(2)  # P2 está na fila, não na RC  # noqa: SLF001
    p._atender_release(4)  # P4 nem pediu  # noqa: SLF001
    assert p.titular_rc == 1, "o titular continua sendo P1"
    assert p.fila_requests == [2], "P2 continua na fila"
    assert enviados == [(1, GRANT)], "nenhum GRANT extra foi enviado"


@pytest.mark.nivel2
def test_n2_10_request_repetido_nao_duplica_a_fila():
    """Pedidos repetidos (ex.: retransmissão) não podem ocupar duas vagas."""
    p, enviados = coordenador_isolado()
    p._atender_request(1)  # noqa: SLF001
    p._atender_request(2)  # noqa: SLF001
    p._atender_request(2)  # noqa: SLF001
    assert p.fila_requests == [2]
    assert enviados == [(1, GRANT)]


@pytest.mark.nivel2
def test_n2_11_request_que_ultrapassa_o_release():
    """O titular pede de novo ANTES de o seu RELEASE chegar (conexões TCP
    distintas não garantem ordem). O novo pedido vai para a fila; reenviar
    o GRANT colocaria dois processos na RC."""
    p, enviados = coordenador_isolado()
    p._atender_request(1)  # noqa: SLF001
    p._atender_request(2)  # noqa: SLF001
    p._atender_request(1)  # REQUEST novo de P1 chegou antes do RELEASE  # noqa: SLF001
    assert p.titular_rc == 1 and p.fila_requests == [2, 1]
    assert enviados == [(1, GRANT)], "nenhum GRANT extra antes do RELEASE"
    p._atender_release(1)  # agora chega o RELEASE atrasado  # noqa: SLF001
    assert p.titular_rc == 2 and p.fila_requests == [1]
    assert enviados == [(1, GRANT), (2, GRANT)]


# 2b) Coordenador + clientes de verdade, pela rede.


@pytest.mark.nivel2
def test_n2_12_request_grant_release(cluster, arbitro):
    """P1 pede a RC a P5, entra, registra no log-server e libera."""
    procs = cluster.subir([5, 1])
    assert procs[1].requisitar_rc("op-teste") is True
    assert esperar_ate(lambda: len(arbitro.saidas) == 1, 2)
    assert [(a["process_id"], a["coordenador_id"], a["mensagem"]) for a in arbitro.acessos] \
        == [(1, 5, "op-teste")]
    # RELEASE é assíncrono: o coordenador o processa em outra thread.
    assert esperar_ate(lambda: procs[5].titular_rc is None and procs[5].fila_requests == [], 2), \
        "após o RELEASE, a RC deve estar livre"
    assert procs[1].estado == "OCIOSO"


@pytest.mark.nivel2
def test_n2_13_coordenador_tambem_pode_usar_a_rc(cluster, arbitro):
    """O coordenador é também um processo comum: pode pedir a RC a si mesmo."""
    procs = cluster.subir([5, 1])
    r = cluster.pedir_em_paralelo([1, 5])
    assert r == {1: True, 5: True}
    assert sorted(a["process_id"] for a in arbitro.acessos) == [1, 5]
    cluster.monitor.assert_exclusao()
    assert esperar_ate(lambda: procs[5].titular_rc is None and procs[5].fila_requests == [], 2)


@pytest.mark.nivel2
def test_n2_14_fila_respeita_a_ordem_de_chegada(cluster):
    """Quatro clientes pedem em sequência rápida: entram na ordem dos pedidos."""
    cluster.subir([5, 1, 2, 3, 4])
    r = cluster.pedir_em_paralelo([3, 1, 4, 2], atraso=0.05)
    assert all(r.values()), f"todos deveriam conseguir: {r}"
    cluster.monitor.assert_exclusao()
    assert cluster.monitor.ordem == [3, 1, 4, 2]


@pytest.mark.nivel2
def test_n2_15_estresse_nunca_dois_na_rc(cluster, arbitro, monkeypatch):
    """5 processos x 3 pedidos cada, todos ao mesmo tempo."""
    monkeypatch.setattr(config, "HOLD_RC", 0.05)
    procs = cluster.subir([5, 1, 2, 3, 4])
    falhas = []

    def rajada(p):
        for k in range(3):
            if not p.requisitar_rc(f"rajada-{k}"):
                falhas.append(p.meu_id)

    ts = [threading.Thread(target=rajada, args=(p,)) for p in procs.values()]
    for t in ts:
        t.start()
    for t in ts:
        t.join(timeout=30)
    assert not falhas, f"pedidos que falharam: {falhas}"
    cluster.monitor.assert_exclusao()
    assert len(cluster.monitor.ordem) == 15
    assert esperar_ate(lambda: len(arbitro.saidas) == 15, 3)
    assert not arbitro.violacoes, arbitro.violacoes


@pytest.mark.nivel2
def test_n2_16_grant_que_nao_foi_pedido_e_ignorado(cluster, rastro):
    """Um cliente OCIOSO, ou um GRANT de quem não é o coordenador, não abre a RC."""
    procs = cluster.subir([5, 2, 1])
    procs[2]._enviar(1, GRANT)  # P2 não é coordenador e P1 nem pediu  # noqa: SLF001
    time.sleep(0.3)
    assert cluster.monitor.ordem == [], "P1 não poderia ter entrado na RC"
    assert procs[1].estado == "OCIOSO"
    # ...e o GRANT espúrio não atrapalha um pedido legítimo em seguida.
    assert procs[1].requisitar_rc("legitimo") is True
    assert cluster.monitor.ordem == [1]


@pytest.mark.nivel2
def test_n2_17_estados_do_cliente(cluster, rastro):
    """O cliente percorre OCIOSO -> AGUARDANDO -> NA_RC -> OCIOSO."""
    procs = cluster.subir([5, 1])
    assert procs[1].requisitar_rc() is True
    estados = [e["estado"] for e in rastro.copia() if e["ev"] == "estado" and e["pid"] == 1]
    assert estados == ["AGUARDANDO", "NA_RC", "OCIOSO"], estados


# =============================================================================
# NÍVEL 3 -- Eleição (Algoritmo do Valentão) e heartbeat
# =============================================================================


@pytest.mark.nivel3
def test_n3_18_election_e_respondido_com_answer(cluster):
    """Quem recebe ELECTION de um menor responde ANSWER (mesmo sendo o coordenador)."""
    procs = cluster.subir([5, 2])
    resp = procs[2]._enviar_e_aguardar(5, ELECTION, timeout=2.0)  # noqa: SLF001
    assert resp is not None, "P5 deveria ter respondido"
    assert resp.tipo == ANSWER and resp.origem == 5


@pytest.mark.nivel3
def test_n3_19_quem_responde_answer_assume_e_se_anuncia(cluster, rastro):
    """Após o ANSWER, P5 conduz sua eleição e anuncia COORDINATOR a P2."""
    procs = cluster.subir([5, 2])
    t0 = time.time()
    procs[2]._enviar_e_aguardar(5, ELECTION)  # noqa: SLF001
    assert esperar_ate(lambda: msgs(rastro, COORDINATOR, de=5, para=2, apos=t0), 3), \
        "P5 deveria ter (re)anunciado COORDINATOR a P2"
    assert cluster.todos_concordam_em(5)


@pytest.mark.nivel3
def test_n3_20_coordinator_atualiza_a_visao_e_responde_ack(cluster):
    procs = cluster.subir([5, 3, 2], coord=5)
    procs[5].parar()
    resp = procs[3]._enviar_e_aguardar(2, COORDINATOR)  # P3 se anuncia para P2  # noqa: SLF001
    assert resp is not None and resp.tipo == ACK
    assert procs[2].coordenador_id == 3


@pytest.mark.nivel3
def test_n3_21_maior_processo_vivo_vence(cluster):
    """P5 caído; P2 inicia a eleição; P4 (maior vivo) vence e todos sabem."""
    procs = cluster.subir([5, 4, 3, 2])
    cluster.derrubar(5)
    procs[2].iniciar_eleicao()
    assert esperar_ate(lambda: cluster.todos_concordam_em(4), 5), cluster.coordenadores()
    assert procs[4].sou_coordenador()


@pytest.mark.nivel3
def test_n3_22_eleicao_em_cascata(cluster):
    """P5 e P4 caídos: a vitória passa a P3."""
    procs = cluster.subir([5, 4, 3, 2, 1])
    cluster.derrubar(5)
    cluster.derrubar(4)
    procs[1].iniciar_eleicao()
    assert esperar_ate(lambda: cluster.todos_concordam_em(3), 6), cluster.coordenadores()


@pytest.mark.nivel3
def test_n3_23_heartbeat_detecta_a_queda_sozinho(cluster):
    """Sem ninguém chamar iniciar_eleicao: o heartbeat detecta e dispara a eleição."""
    cluster.subir([5, 3, 2])
    t0 = time.monotonic()
    cluster.derrubar(5)
    limite = config.INTERVALO_HEARTBEAT + config.TIMEOUT_RESPOSTA + 2 * config.TIMEOUT_ELEICAO + 1
    assert esperar_ate(lambda: cluster.todos_concordam_em(3), limite), \
        f"em {limite:.1f}s ninguém detectou a queda: {cluster.coordenadores()}"
    print(f"queda detectada e resolvida em {time.monotonic() - t0:.2f}s")


@pytest.mark.nivel3
def test_n3_24_heartbeat_nao_dispara_eleicao_sem_falha(cluster, rastro):
    """Com o coordenador vivo, o heartbeat só pinga: nenhuma eleição espúria."""
    cluster.subir([5, 3, 2, 1])
    t0 = time.time()
    time.sleep(6 * config.INTERVALO_HEARTBEAT)
    assert msgs(rastro, PING, apos=t0), "o heartbeat deveria estar enviando PINGs"
    assert not msgs(rastro, ELECTION, apos=t0), "houve ELECTION sem falha nenhuma"
    assert cluster.todos_concordam_em(5)


@pytest.mark.nivel3
def test_n3_25_eleicoes_simultaneas_convergem(cluster):
    """P1, P2 e P3 iniciam eleições ao mesmo tempo: ainda assim, um só vencedor."""
    procs = cluster.subir([5, 4, 3, 2, 1])
    cluster.derrubar(5)
    ts = [threading.Thread(target=procs[p].iniciar_eleicao) for p in (1, 2, 3)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(timeout=10)
    assert esperar_ate(lambda: cluster.todos_concordam_em(4), 5), cluster.coordenadores()


@pytest.mark.nivel3
def test_n3_26_processo_sozinho_se_elege_e_usa_a_rc(cluster, arbitro):
    procs = cluster.subir([5, 2])
    cluster.derrubar(5)
    assert procs[2].requisitar_rc("sozinho") is True
    assert procs[2].sou_coordenador()
    assert arbitro.acessos[-1]["coordenador_id"] == 2


# =============================================================================
# NÍVEL 4 -- Falha do coordenador e continuidade (Cenário 2)
# =============================================================================


@pytest.mark.nivel4
def test_n4_27_falha_e_reentrada_na_rc(cluster, arbitro):
    """Roteiro do Cenário 2: acesso sob P5, queda de P5, novo acesso sob P3."""
    procs = cluster.subir([5, 3, 2])
    assert procs[2].requisitar_rc("primeiro-acesso") is True
    cluster.derrubar(5)
    assert procs[2].requisitar_rc("apos-falha") is True
    assert [a["coordenador_id"] for a in arbitro.acessos] == [5, 3]
    assert procs[3].sou_coordenador()


@pytest.mark.nivel4
def test_n4_28_pedido_durante_a_eleicao_nao_desiste(cluster):
    """Pedir a RC logo após a queda, com o heartbeat já elegendo outro, deve funcionar.

    Armadilha comum: se iniciar_eleicao() retornar na hora porque "já há
    uma eleição em andamento", o cliente gasta as tentativas com o
    coordenador morto e desiste.
    """
    procs = cluster.subir([5, 4, 3, 2, 1])
    cluster.derrubar(5)
    r = cluster.pedir_em_paralelo([1, 2, 3], atraso=0.0)
    assert r == {1: True, 2: True, 3: True}, r
    cluster.monitor.assert_exclusao()


@pytest.mark.nivel4
def test_n4_29_fila_do_coordenador_morto_e_redirecionada(cluster, monkeypatch):
    """Clientes na fila de P5 quando ele cai refazem o pedido ao novo coordenador.

    Eles NÃO podem ficar esperando TIMEOUT_GRANT: o pedido morreu com P5.
    """
    monkeypatch.setattr(config, "HOLD_RC", 0.6)
    procs = cluster.subir([5, 4, 3, 2, 1])
    resultados: Dict[int, float] = {}

    def pedir(pid):
        procs[pid].requisitar_rc(f"fila-P{pid}")
        resultados[pid] = time.monotonic()

    ts = [threading.Thread(target=pedir, args=(pid,)) for pid in (1, 2, 3)]
    for t in ts:
        t.start()
        time.sleep(0.05)
    assert esperar_ate(lambda: procs[5].fila_requests == [2, 3], 2), "P2 e P3 deveriam estar na fila"
    t_queda = time.monotonic()
    cluster.derrubar(5)
    for t in ts:
        t.join(timeout=config.TIMEOUT_GRANT * 2)

    assert set(resultados) == {1, 2, 3}
    for pid in (2, 3):
        atraso = resultados[pid] - t_queda
        assert atraso < config.TIMEOUT_GRANT / 2, \
            f"P{pid} levou {atraso:.1f}s: esperou o TIMEOUT_GRANT em vez de refazer o pedido"
    assert cluster.todos_concordam_em(4)


# =============================================================================
# NÍVEL 5 -- Desafios (bônus)
# =============================================================================


@pytest.mark.nivel5
@pytest.mark.desafio
def test_n5_30_novo_coordenador_reconhece_quem_esta_na_rc(cluster, arbitro, monkeypatch):
    """P5 cai com P1 DENTRO da RC. O novo coordenador não pode deixar P2 entrar
    antes de P1 sair: no anúncio COORDINATOR, P1 responde ACK com NA_RC."""
    monkeypatch.setattr(config, "HOLD_RC", 1.5)
    procs = cluster.subir([5, 4, 2, 1])
    t1 = threading.Thread(target=procs[1].requisitar_rc, args=("dentro",))
    t1.start()
    assert esperar_ate(lambda: procs[1].estado == "NA_RC", 2)
    cluster.derrubar(5)
    assert esperar_ate(lambda: procs[4].sou_coordenador() and procs[4].titular_rc == 1, 5), \
        "P4 deveria saber que P1 está na RC (titular_rc == 1)"
    monkeypatch.setattr(config, "HOLD_RC", 0.1)
    assert procs[2].requisitar_rc("depois") is True
    t1.join(timeout=5)
    cluster.monitor.assert_exclusao()
    assert not arbitro.violacoes, arbitro.violacoes


@pytest.mark.nivel5
@pytest.mark.desafio
def test_n5_31_antigo_coordenador_volta_e_retoma(cluster, arbitro):
    """P5 cai, P4 assume; P5 volta e, pelo Valentão, retoma a coordenação."""
    procs = cluster.subir([5, 4, 3])
    cluster.derrubar(5)
    assert esperar_ate(lambda: cluster.todos_concordam_em(4), 5)
    novo5 = Processo(5)          # P5 "reinicia" sem saber o que houve
    cluster.monitor.envolver(novo5)
    novo5.iniciar()
    cluster.procs[5] = novo5
    assert esperar_ate(lambda: cluster.todos_concordam_em(5), 5), \
        f"dois coordenadores? {cluster.coordenadores()}"
    assert procs[3].requisitar_rc("apos-retorno") is True
    assert arbitro.acessos[-1]["coordenador_id"] == 5


@pytest.mark.nivel5
@pytest.mark.desafio
def test_n5_32_anuncio_de_um_menor_e_contestado(cluster, rastro):
    """P3 vivo recebe COORDINATOR de P1 (menor): contesta e se reafirma."""
    procs = cluster.subir([3, 1], coord=3)
    t0 = time.time()
    procs[1]._enviar_e_aguardar(3, COORDINATOR)  # noqa: SLF001
    assert esperar_ate(lambda: msgs(rastro, COORDINATOR, de=3, para=1, apos=t0), 4), \
        "P3 deveria ter reafirmado sua coordenação para P1"
    assert procs[3].sou_coordenador() and procs[1].coordenador_id == 3


@pytest.mark.nivel5
@pytest.mark.desafio
def test_n5_33_grant_inesperado_do_coordenador_e_devolvido(cluster):
    """Se o coordenador acha que a RC é de P1, mas P1 não pediu, P1 a devolve."""
    procs = cluster.subir([5, 1])
    procs[5]._atender_request(1)  # simula um pedido "fantasma" de P1  # noqa: SLF001
    assert esperar_ate(lambda: procs[5].titular_rc is None, 2), \
        "P1 deveria ter devolvido a RC com RELEASE"
    assert cluster.monitor.ordem == []


@pytest.mark.nivel5
@pytest.mark.desafio
def test_n5_34_reeleicao_do_mesmo_coordenador_preserva_a_fila(cluster, monkeypatch):
    """Uma eleição espúria (P5 vivo) não pode apagar a fila de P5."""
    monkeypatch.setattr(config, "HOLD_RC", 0.8)
    procs = cluster.subir([5, 3, 2, 1])
    t1 = threading.Thread(target=procs[1].requisitar_rc)
    t1.start()
    time.sleep(0.1)
    t2 = threading.Thread(target=procs[2].requisitar_rc)
    t2.start()
    assert esperar_ate(lambda: procs[5].fila_requests == [2], 2)
    procs[3]._enviar_e_aguardar(5, ELECTION)  # eleição espúria  # noqa: SLF001
    inicio = time.monotonic()
    t2.join(timeout=config.TIMEOUT_GRANT)
    assert time.monotonic() - inicio < 3, "P2 perdeu o lugar na fila e esperou o TIMEOUT_GRANT"
    t1.join(timeout=3)
    assert cluster.monitor.ordem == [1, 2]
