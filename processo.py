"""
processo.py  (ESQUELETO)
========================

=============================================================================
ESQUELETO -- o que VOCÊ implementa (procure por "TODO"; siga os níveis dos testes)
=============================================================================

  Parte A  coordenador     _atender_request, _atender_release        (nível 2)
  Parte B  cliente         requisitar_rc, _receber_grant             (nível 2)
  Parte C  eleição         _receber_election, _conduzir_eleicao,
                           _tornar_coordenador, _receber_coordinator (nível 3)
  Parte D  heartbeat       _heartbeat                                (nível 3)

Em cada um desses métodos, a docstring é o CONTRATO (o que o método deve
garantir) e o TODO traz DICAS. O "como" é com você.

Todo o resto (rede, log, observação, CLI, _conceder, _aguardar_grant,
_executar_rc, _liberar_rc, iniciar_eleicao, _nova_thread) está PRONTO. Leia
as docstrings dos métodos prontos: eles são as ferramentas que você vai usar.
Itens marcados com [DESAFIO] valem bônus (nível 5 dos testes).

Um processo do sistema distribuído. Cada instância desta classe (seja em
terminais separados, seja em threads nos testes) é um "nó" com:

  * um SERVIDOR TCP próprio, que recebe mensagens dos outros processos;
  * um papel de COORDENADOR (se for o coordenador atual), que serializa o
    acesso à região crítica (RC) com uma fila FIFO;
  * um papel de CLIENTE, que pede a RC ao coordenador, entra, trabalha e sai;
  * um HEARTBEAT, que pinga o coordenador periodicamente e dispara a eleição
    quando ele não responde;
  * o ALGORITMO DO VALENTÃO (Bully) para eleger um novo coordenador.

Threads de cada processo
------------------------

    thread principal ... linha de comando (request / status / eleicao / sair)
    srv-P<id> .......... aceita conexões TCP; cada conexão ganha uma thread
                         curta que decodifica a mensagem e chama _despachar()
    hb-P<id> ........... heartbeat: PING no coordenador a cada intervalo
    (sob demanda) ...... eleições e pedidos de RC disparados pela CLI

Como várias threads tocam o mesmo estado, cada grupo de estado tem um lock:

    _lock_rc        titular_rc, fila_requests          (papel de coordenador)
    _lock_cliente   estado, _coord_do_pedido, GRANT    (papel de cliente)
    _lock_eleicao   _em_eleicao                        (eleição)

Máquina de estados do CLIENTE
-----------------------------

                 requisitar_rc()            GRANT do coordenador
      OCIOSO  -------------------->  AGUARDANDO  ------------------->  NA_RC
        ^                              |   ^                            |
        |    desistiu (tentativas)     |   | coordenador mudou:         |
        +------------------------------+   +- refaz o REQUEST           |
        |                                                               |
        +------------------------- RELEASE (saiu da RC) ----------------+

Uso:
    python processo.py <ID> [--coordenador <ID>] [--verbose] [--sem-cores]
Exemplo:
    python processo.py 3
"""

from __future__ import annotations

import argparse
import datetime as dt
import random
import socket
import sys
import threading
import time
from typing import Callable, List, Optional

import grpc

import config
import log_service_pb2 as pb
import log_service_pb2_grpc as pb_grpc
import terminal as T
from config import (
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
    Mensagem,
    montar,
    parsear,
)

# Estados do papel de CLIENTE (ver diagrama no topo do arquivo).
OCIOSO = "OCIOSO"
AGUARDANDO = "AGUARDANDO"
NA_RC_ESTADO = "NA_RC"

# Resultados possíveis de _aguardar_grant().
CONCEDIDO = "CONCEDIDO"      # recebi o GRANT: posso entrar na RC
COORD_MUDOU = "COORD_MUDOU"  # um novo coordenador foi eleito: refazer o pedido
ESGOTADO = "ESGOTADO"        # TIMEOUT_GRANT estourou: desconfiar do coordenador
ENCERRADO = "ENCERRADO"      # este processo foi parado

#: Mensagens que não aparecem no log do terminal (só com --verbose), para não
#: poluir a saída a cada heartbeat.
_SILENCIOSAS = {PING, PONG}


class Processo:
    """Um processo com exclusão mútua centralizada e eleição pelo Valentão."""

    #: Gancho opcional de observação. Se definido, recebe um dict a cada
    #: evento relevante (mensagem enviada, entrada/saída da RC, mudança de
    #: coordenador ou de estado). Usado pelos testes e por visualizacao.py.
    #: Não interfere no algoritmo.
    observador: Optional[Callable[[dict], None]] = None

    #: Mostrar PING/PONG no terminal? (opção --verbose)
    verbose: bool = False

    #: Suprimir todo o log no terminal (usado por cenarios.py sem --log).
    silencioso: bool = False

    # ==================================================================
    # Construção e ciclo de vida                      (PRONTO, não mexer)
    # ==================================================================

    def __init__(self, meu_id: int, coordenador_inicial: Optional[int] = None):
        if meu_id not in config.PORTAS:
            raise ValueError(f"ID {meu_id} não está em config.PORTAS={list(config.PORTAS)}")
        if coordenador_inicial is None:
            coordenador_inicial = config.COORDENADOR_INICIAL
        self.meu_id: int = meu_id
        self.minha_porta: int = config.PORTAS[meu_id]
        self._coordenador_id: int = coordenador_inicial

        # --- Papel de COORDENADOR (só tem significado se eu for o coordenador)
        self._lock_rc = threading.Lock()
        self.titular_rc: Optional[int] = None   # quem está com a RC agora
        self.fila_requests: List[int] = []      # quem espera, em ordem FIFO

        # --- Papel de CLIENTE
        self._lock_cliente = threading.Lock()
        self._estado: str = OCIOSO
        self._coord_do_pedido: Optional[int] = None  # a quem pedi a RC
        self._evento_grant = threading.Event()        # "acorda" quem espera o GRANT

        # --- ELEIÇÃO
        self._lock_eleicao = threading.Lock()
        self._em_eleicao: bool = False
        self._fim_eleicao = threading.Event()        # setado quando a eleição termina
        self._fim_eleicao.set()
        self._anuncio_recebido = threading.Event()   # setado ao aceitar um COORDINATOR

        # --- Infraestrutura
        self._parado = threading.Event()
        self._parado.set()
        self._socket_servidor: Optional[socket.socket] = None
        self._canal_log: Optional[grpc.Channel] = None
        self._threads: List[threading.Thread] = []

    @property
    def ativo(self) -> bool:
        return not self._parado.is_set()

    def iniciar(self) -> None:
        """Sobe o servidor TCP e o heartbeat.

        Se este processo começa achando que É o coordenador (ex.: P5 subindo,
        ou voltando depois de uma falha), ele não assume o posto às cegas:
        roda uma eleição. É a regra do Valentão para quem (re)entra no
        sistema; sem ela, um P5 que volta conviveria com o coordenador
        eleito na sua ausência -> dois coordenadores.
        """
        self._socket_servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket_servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket_servidor.bind((config.HOST, self.minha_porta))
        self._socket_servidor.listen(32)
        self._socket_servidor.settimeout(0.2)
        self._parado.clear()

        self._nova_thread(self._servir, f"srv-P{self.meu_id}")
        self._nova_thread(self._heartbeat, f"hb-P{self.meu_id}")

        quem = "EU" if self.sou_coordenador() else f"P{self.coordenador_id}"
        self._log(f"escutando em {config.HOST}:{self.minha_porta} | coordenador presumido = {quem}")
        self._emitir("inicio", coord=self.coordenador_id)

        if self.sou_coordenador():
            self._nova_thread(self.iniciar_eleicao, f"eleicao-inicial-P{self.meu_id}")

    def parar(self) -> None:
        """Encerra o processo. Nos cenários, isto SIMULA A FALHA (crash)."""
        if not self.ativo:
            return
        self._parado.set()
        if self._socket_servidor is not None:
            try:
                self._socket_servidor.close()
            except OSError:
                pass
            self._socket_servidor = None
        if self._canal_log is not None:
            self._canal_log.close()
            self._canal_log = None
        self._evento_grant.set()  # acorda quem esteja esperando (para sair)
        self._log(T.cor("encerrado.", "vermelho"), mesmo_parado=True)
        self._emitir("parada")

    def _nova_thread(self, alvo: Callable[[], None], nome: str) -> threading.Thread:
        """Cria uma thread daemon que roda ``alvo``.

        Se ``alvo`` ainda não foi implementado (NotImplementedError), avisa
        no log em vez de despejar um traceback -- útil enquanto o esqueleto
        está incompleto.
        """

        def protegido() -> None:
            try:
                alvo()
            except NotImplementedError as e:
                self._log(T.cor(f"[ainda não implementado] {e}", "vermelho"))

        t = threading.Thread(target=protegido, name=nome, daemon=True)
        t.start()
        self._threads.append(t)
        return t

    # ==================================================================
    # Atributos observados                            (PRONTO, não mexer)
    # ==================================================================
    # Atribuir ``self.coordenador_id = x`` ou ``self.estado = y`` funciona
    # como um atributo comum, mas também registra o evento para as
    # ferramentas de visualização e para os testes.

    @property
    def coordenador_id(self) -> int:
        return self._coordenador_id

    @coordenador_id.setter
    def coordenador_id(self, novo: int) -> None:
        antigo = self._coordenador_id
        self._coordenador_id = novo
        if novo != antigo:
            self._emitir("coord", coord=novo, antes=antigo)

    @property
    def estado(self) -> str:
        return self._estado

    @estado.setter
    def estado(self, novo: str) -> None:
        antigo = self._estado
        self._estado = novo
        if novo != antigo:
            self._emitir("estado", estado=novo, antes=antigo)

    def sou_coordenador(self) -> bool:
        return self.coordenador_id == self.meu_id

    # ==================================================================
    # Log e observação                                (PRONTO, não mexer)
    # ==================================================================

    def _log(self, msg: str, mesmo_parado: bool = False) -> None:
        if Processo.silencioso or not (self.ativo or mesmo_parado):
            return  # um processo parado (falho) não fala mais nada
        ts = dt.datetime.now().strftime("%H:%M:%S.%f")[:-3]
        papel = T.cor("COORD", "negrito") if self.sou_coordenador() else "cli  "
        print(f"{T.cor(ts, 'cinza')} {T.rotulo_processo(self.meu_id)} {papel} | {msg}", flush=True)

    def _emitir(self, evento: str, **dados) -> None:
        obs = Processo.observador
        if obs is not None:
            try:
                obs({"t": time.time(), "ev": evento, "pid": self.meu_id, **dados})
            except Exception:  # o observador nunca pode derrubar o processo
                pass

    def _log_trafego(self, seta: str, msg: Mensagem, outro: int, ok: bool = True) -> None:
        if msg.tipo in _SILENCIOSAS and not (self.verbose or not ok):
            return
        extra = f" [{msg.extra}]" if msg.extra else ""
        texto = f"{seta} {T.rotulo_mensagem(msg.tipo)}{extra} {'para' if seta == '->' else 'de'} P{outro}"
        if not ok:
            texto += T.cor("  (falhou: processo inalcançável)", "vermelho")
        self._log(texto)

    # ==================================================================
    # Rede: servidor                                  (PRONTO, não mexer)
    # ==================================================================

    def _servir(self) -> None:
        """Aceita conexões e trata cada uma em uma thread própria."""
        while self.ativo:
            sock = self._socket_servidor
            if sock is None:
                break
            try:
                conn, _ = sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._tratar_conexao, args=(conn,), daemon=True).start()

    def _tratar_conexao(self, conn: socket.socket) -> None:
        """Lê UMA mensagem, despacha, e devolve a resposta (se houver)."""
        with conn:
            try:
                conn.settimeout(config.TIMEOUT_RESPOSTA)
                linha = self._ler_linha(conn)
                if not linha or not self.ativo:
                    return  # processo "morto" não responde a nada
                msg = parsear(linha)
            except (ValueError, OSError):
                return
            self._log_trafego("<-", msg, msg.origem)
            try:
                resposta = self._despachar(msg.tipo, msg.origem, msg.extra)
            except NotImplementedError as e:
                self._log(T.cor(f"[ainda não implementado] {e}", "vermelho"))
                return
            if resposta is not None and self.ativo:
                try:
                    conn.sendall(resposta)
                    r = parsear(resposta.decode("utf-8"))
                    self._emitir("msg", de=self.meu_id, para=msg.origem, tipo=r.tipo,
                                 extra=r.extra, ok=True)
                except (OSError, ValueError):
                    pass

    @staticmethod
    def _ler_linha(sock: socket.socket, limite: int = 4096) -> str:
        buf = b""
        while not buf.endswith(b"\n") and len(buf) < limite:
            pedaco = sock.recv(1024)
            if not pedaco:
                break
            buf += pedaco
        return buf.decode("utf-8", errors="replace")

    # ==================================================================
    # Rede: cliente                                   (PRONTO, não mexer)
    # ==================================================================

    def _enviar(self, destino: int, tipo: str, extra: str = "") -> bool:
        """Envia uma mensagem ASSÍNCRONA (sem resposta).

        Retorna True se a mensagem foi ENTREGUE ao destino (conexão aceita e
        dados enviados) e False se o destino está inalcançável.
        """
        return self._transmitir(destino, tipo, extra, aguardar=False) is not None

    def _enviar_e_aguardar(
        self, destino: int, tipo: str, extra: str = "", timeout: Optional[float] = None
    ) -> Optional[Mensagem]:
        """Envia uma mensagem SÍNCRONA e espera UMA linha de resposta.

        Retorna a ``Mensagem`` de resposta, ou None se o destino está
        inalcançável, não respondeu dentro do timeout, ou fechou a conexão
        sem responder.
        """
        resp = self._transmitir(destino, tipo, extra, aguardar=True, timeout=timeout)
        return resp if isinstance(resp, Mensagem) else None

    def _transmitir(self, destino, tipo, extra, aguardar, timeout=None):
        """Implementação comum: retorna None (falha), True (entregue) ou Mensagem."""
        if destino == self.meu_id:
            raise ValueError("um processo não envia mensagens de rede para si mesmo")
        if not self.ativo:
            return None  # processo parado (falho) não envia nada
        if timeout is None:
            timeout = config.TIMEOUT_RESPOSTA
        msg = Mensagem(tipo, self.meu_id, extra)
        resultado = None
        conectou = False
        try:
            with socket.create_connection((config.HOST, config.PORTAS[destino]), timeout=timeout) as s:
                conectou = True
                self._log_trafego("->", msg, destino)
                self._emitir("msg", de=self.meu_id, para=destino, tipo=tipo, extra=extra, ok=True)
                s.sendall(montar(tipo, self.meu_id, extra))
                resultado = True
                if aguardar:
                    linha = self._ler_linha(s)
                    resultado = parsear(linha) if linha else True
        except (OSError, ValueError):
            pass
        if not conectou:
            self._log_trafego("->", msg, destino, ok=False)
            self._emitir("msg", de=self.meu_id, para=destino, tipo=tipo, extra=extra, ok=False)
        if isinstance(resultado, Mensagem):
            self._log_trafego("<-", resultado, destino)
        return resultado

    # ==================================================================
    # Despacho de mensagens recebidas
    # ==================================================================

    def _despachar(self, tipo: str, origem: int, extra: str) -> Optional[bytes]:
        """Decide o que fazer com uma mensagem recebida.

        Retorna os bytes da RESPOSTA (enviada na mesma conexão) ou None se a
        mensagem não tem resposta.
        """
        # ------------------------------------------------ prontos para você
        if tipo == PING:
            return montar(PONG, self.meu_id)

        if tipo == REQUEST:
            if not self.sou_coordenador():
                self._log(T.cor(f"REQUEST de P{origem} ignorado: não sou o coordenador", "vermelho"))
                return None
            self._atender_request(origem)
            return None

        if tipo == RELEASE:
            if not self.sou_coordenador():
                self._log(T.cor(f"RELEASE de P{origem} ignorado: não sou o coordenador", "vermelho"))
                return None
            self._atender_release(origem)
            return None

        if tipo == GRANT:
            self._receber_grant(origem)
            return None

        if tipo == ELECTION:
            return self._receber_election(origem)

        if tipo == COORDINATOR:
            return self._receber_coordinator(origem)

        self._log(T.cor(f"tipo desconhecido {tipo!r} de P{origem}", "vermelho"))
        return None

    # ==================================================================
    # PARTE A -- Papel de COORDENADOR (exclusão mútua centralizada)
    # ==================================================================

    def _atender_request(self, origem: int) -> None:
        """O coordenador recebeu um REQUEST de ``origem`` e decide o destino do pedido.

        Requisitos (verificados pelos testes do nível 2):
          * no máximo UM processo é titular da RC (``titular_rc``) em cada instante;
          * quem não pode entrar agora espera em ``fila_requests``, em ordem de chegada;
          * um processo que já espera na fila não ganha uma segunda vaga;
          * um processo pode pedir de novo enquanto ainda consta como titular (veja
            a seção 7.1 do enunciado). Isso NUNCA pode resultar em um segundo GRANT
            antes de o RELEASE dele ser processado;
          * só recebe GRANT quem acabou de se tornar o titular.
        """
        conceder = False
        with self._lock_rc:
            if not self.sou_coordenador():
                return
            if self.titular_rc is None:
                self.titular_rc = origem
                conceder = True
                self._log(f"RC livre -> concedo a P{origem}")
            elif origem in self.fila_requests:
                self._log(f"REQUEST duplicado de P{origem} ignorado | fila: {self._fila_txt()}")
            else:
                self.fila_requests.append(origem)
                self._log(
                    f"RC ocupada por P{self.titular_rc} -> P{origem} vai para a fila | fila: {self._fila_txt()}"
                )
        if conceder:
            self._conceder(origem)

    def _atender_release(self, origem: int) -> None:
        """O coordenador recebeu um RELEASE de ``origem``.

        Requisitos (nível 2):
          * só quem detém a RC pode liberá-la: um RELEASE de qualquer outro processo
            não altera o estado (registre um aviso no log);
          * ao ser liberada, a RC passa ao pedido mais antigo da fila, que é avisado;
          * sem ninguém esperando, a RC fica livre.
        """
        proximo: Optional[int] = None
        with self._lock_rc:
            if not self.sou_coordenador():
                return
            if self.titular_rc != origem:
                titular_txt = f"P{self.titular_rc}" if self.titular_rc is not None else "ninguém"
                self._log(
                    T.cor(f"RELEASE ignorado de P{origem} (titular atual: {titular_txt})", "amarelo")
                )
                return
            if self.fila_requests:
                proximo = self.fila_requests.pop(0)
                self.titular_rc = proximo
                self._log(
                    f"P{origem} liberou a RC -> concedo a P{proximo} | fila: {self._fila_txt()}"
                )
            else:
                self.titular_rc = None
                self._log(f"P{origem} liberou a RC -> RC livre")
        if proximo is not None:
            self._conceder(proximo)

    def _conceder(self, pid: int) -> None:
        """Entrega o GRANT a ``pid`` (PRONTO).

        * Se ``pid`` sou eu (o coordenador também pode ser cliente), o GRANT
          é entregue localmente, sem rede.
        * Se ``pid`` está inalcançável, ele caiu antes de usar a RC: a RC é
          liberada em seu nome, para que a fila não trave.
        """
        if pid == self.meu_id:
            self._receber_grant(self.meu_id)
            return
        if not self._enviar(pid, GRANT):
            self._log(T.cor(f"P{pid} caiu antes de usar a RC; libero em nome dele", "vermelho"))
            self._atender_release(pid)

    def _fila_txt(self) -> str:
        return "[" + ", ".join(f"P{p}" for p in self.fila_requests) + "]"

    # ==================================================================
    # PARTE B -- Papel de CLIENTE
    # ==================================================================

    def requisitar_rc(self, mensagem: str = "trabalho", tentativas: int = 3) -> bool:
        """Pede a RC, entra, trabalha e sai. Retorna True se conseguiu.

        Bloqueia a thread chamadora até o fim do acesso (ou da desistência). O
        trecho já pronto acima garante uma requisição por vez e põe o estado em
        AGUARDANDO; o ``finally`` abaixo o devolve a OCIOSO.

        Requisitos (níveis 2 e 4):
          * funciona também quando este processo É o coordenador;
          * se o coordenador estiver inalcançável ou não conceder a tempo, este
            processo provoca uma eleição e tenta de novo, até ``tentativas`` falhas;
          * se o coordenador MUDAR enquanto o pedido espera (ex.: o antigo caiu com
            este processo na fila), o pedido é refeito ao novo coordenador logo,
            sem esperar ``TIMEOUT_GRANT``, e sem que isso conte como falha;
          * concedida a RC, executa o trabalho e a libera.
        """
        with self._lock_cliente:
            if self.estado != OCIOSO:
                self._log(T.cor(f"já existe um pedido em andamento (estado {self.estado})", "vermelho"))
                return False
            self.estado = AGUARDANDO
        try:
            falhas = 0
            while falhas < tentativas and self.ativo:
                coord = self.coordenador_id
                with self._lock_cliente:
                    self._evento_grant.clear()
                    self._coord_do_pedido = coord

                if coord == self.meu_id:
                    self._atender_request(self.meu_id)
                elif not self._enviar(coord, REQUEST):
                    with self._lock_cliente:
                        self._coord_do_pedido = None
                    if not self.ativo:
                        return False
                    if self.coordenador_id != coord:
                        self._log(
                            f"coordenador mudou para P{self.coordenador_id}; refazendo REQUEST"
                        )
                        continue
                    falhas += 1
                    self._log(
                        T.cor(
                            f"coordenador P{coord} inalcançável ({falhas}/{tentativas}); iniciando eleição",
                            "amarelo",
                        )
                    )
                    if falhas < tentativas:
                        self.iniciar_eleicao()
                    continue

                resultado = self._aguardar_grant(coord)
                if resultado == CONCEDIDO:
                    try:
                        self._executar_rc(mensagem)
                    finally:
                        self._liberar_rc()
                    return True
                if resultado == COORD_MUDOU:
                    self._log(
                        f"coordenador mudou (P{coord} -> P{self.coordenador_id}); refazendo REQUEST"
                    )
                    continue
                if resultado == ESGOTADO:
                    falhas += 1
                    self._log(
                        T.cor(
                            f"timeout esperando GRANT de P{coord} ({falhas}/{tentativas}); iniciando eleição",
                            "amarelo",
                        )
                    )
                    if self.ativo and falhas < tentativas:
                        self.iniciar_eleicao()
                    continue
                return False
            return False
        finally:
            with self._lock_cliente:
                self.estado = OCIOSO
                self._coord_do_pedido = None

    def _receber_grant(self, origem: int) -> None:
        """Chegou um GRANT de ``origem`` (pela rede, ou localmente, via _conceder).

        Requisitos (nível 2):
          * um GRANT só abre a RC para um processo que está ESPERANDO por ele e se
            veio do coordenador a quem o pedido foi feito;
          * qualquer outro GRANT (atrasado, de um coordenador já substituído, de quem
            não é coordenador...) é ignorado e registrado no log;
          * ao aceitar, a máquina de estados do cliente avança (Figura 6 do
            enunciado) e a thread que espera em requisitar_rc é acordada.

        [DESAFIO, teste n5_33] Um GRANT do coordenador ATUAL para um processo que
        não pediu nada faria o coordenador esperar para sempre por um RELEASE que
        nunca virá. Este processo deve desfazer o equívoco.
        """
        devolver = False
        with self._lock_cliente:
            if self.estado == AGUARDANDO and self._coord_do_pedido == origem:
                self.estado = NA_RC_ESTADO
                self._evento_grant.set()
                self._log(T.cor(f"GRANT aceito de P{origem}", "verde"))
                return
            estado_atual = self.estado
            esperado = self._coord_do_pedido
            if origem == self.coordenador_id and estado_atual != NA_RC_ESTADO:
                devolver = True

        self._log(
            T.cor(
                f"GRANT ignorado de P{origem} (estado={estado_atual}, pedido_para={esperado})",
                "amarelo",
            )
        )
        if devolver and self.ativo:
            self._log(f"devolvendo GRANT inesperado ao coordenador atual P{origem} (RELEASE)")
            if origem == self.meu_id:
                self._atender_release(self.meu_id)
            else:
                self._enviar(origem, RELEASE)

    def _aguardar_grant(self, coord: int) -> str:
        """Espera o GRANT do pedido feito a ``coord`` (PRONTO).

        Em vez de um único ``wait(TIMEOUT_GRANT)``, espera em fatias curtas
        e, a cada fatia, verifica se o coordenador mudou. Assim, se o
        coordenador cai enquanto este cliente está na fila, o cliente refaz
        o pedido ao novo coordenador em ~1 s, e não em TIMEOUT_GRANT (60 s).

        Retorna CONCEDIDO, COORD_MUDOU, ESGOTADO ou ENCERRADO.
        """
        limite = time.monotonic() + config.TIMEOUT_GRANT
        while True:
            if self._evento_grant.wait(timeout=0.1):
                return CONCEDIDO if self.ativo else ENCERRADO
            if not self.ativo:
                motivo = ENCERRADO
            elif self.coordenador_id != coord:
                motivo = COORD_MUDOU
            elif time.monotonic() >= limite:
                motivo = ESGOTADO
            else:
                continue
            # Fecha a janela de corrida: decide sob o lock e invalida o
            # pedido, para que um GRANT atrasado dele seja ignorado.
            with self._lock_cliente:
                if self._evento_grant.is_set() and self.ativo:
                    return CONCEDIDO
                self._coord_do_pedido = None
            return motivo

    def _executar_rc(self, mensagem: str) -> None:
        """A REGIÃO CRÍTICA propriamente dita (PRONTO).

        Registra a entrada no servidor de log (gRPC), "trabalha" por
        ``config.HOLD_RC`` segundos e registra a saída.
        """
        quem_autorizou = self._coord_do_pedido or self.coordenador_id
        self._emitir("rc_entrada", coord=quem_autorizou, mensagem=mensagem)
        self._log(T.cor(f"######## ENTREI NA REGIÃO CRÍTICA ({mensagem}) ########", "verde_forte"))
        self._registrar_no_log(pb.ENTRADA, quem_autorizou, mensagem)
        self._parado.wait(config.HOLD_RC)  # "trabalho"; interrompido se o processo parar
        if self.ativo:
            self._registrar_no_log(pb.SAIDA, quem_autorizou, mensagem)
            self._log(T.cor("######## SAÍ DA REGIÃO CRÍTICA ########", "verde"))
        self._emitir("rc_saida", coord=quem_autorizou, mensagem=mensagem, falhou=not self.ativo)

    def _liberar_rc(self) -> None:
        """Sai da RC e avisa o coordenador ATUAL (PRONTO).

        O estado volta a OCIOSO ANTES do RELEASE: se um novo coordenador
        perguntar (COORDINATOR/ACK) nesse meio-tempo, a resposta é coerente.
        Um processo parado (falha dentro da RC) não envia nada.
        """
        with self._lock_cliente:
            self.estado = OCIOSO
        if not self.ativo:
            return
        coord = self.coordenador_id
        if coord == self.meu_id:
            self._atender_release(self.meu_id)
        elif not self._enviar(coord, RELEASE):
            self._log(T.cor(f"RELEASE não entregue: P{coord} caiu (o próximo coordenador começa do zero)", "vermelho"))

    def _registrar_no_log(self, evento: int, coord: int, mensagem: str) -> None:
        try:
            if self._canal_log is None:
                alvo = f"{config.LOG_SERVER_HOST}:{config.LOG_SERVER_PORTA}"
                self._canal_log = grpc.insecure_channel(alvo)
            stub = pb_grpc.LogServiceStub(self._canal_log)
            req = pb.AcessoRequest(
                process_id=self.meu_id,
                coordenador_id=coord,
                timestamp=dt.datetime.now().isoformat(timespec="milliseconds"),
                mensagem=mensagem,
                evento=evento,
            )
            resp = stub.RegistrarAcesso(req, timeout=2.0)
            self._log(T.cor(f"log-server: {resp.confirmacao}", "cinza"))
        except (grpc.RpcError, ValueError, AttributeError) as e:
            codigo = e.code() if isinstance(e, grpc.RpcError) else e
            self._log(T.cor(f"log-server indisponível ({codigo}); sigo sem registrar", "vermelho"))

    # ==================================================================
    # PARTE C -- Eleição: Algoritmo do Valentão (Bully)
    # ==================================================================

    def iniciar_eleicao(self) -> None:
        """Ponto de entrada da eleição (PRONTO).

        Garante que cada processo conduz no máximo UMA eleição por vez. Se
        já houver uma em andamento (disparada por outra thread: heartbeat,
        CLI, ELECTION recebido...), NÃO retorna de imediato: espera ela
        terminar. Assim quem chamou (ex.: requisitar_rc) volta já conhecendo
        o novo coordenador, em vez de tentar de novo com o antigo.
        """
        with self._lock_eleicao:
            outra_em_andamento = self._em_eleicao
            if not outra_em_andamento:
                self._em_eleicao = True
                self._fim_eleicao.clear()
        if outra_em_andamento:
            self._fim_eleicao.wait(timeout=config.TIMEOUT_ELEICAO * 6)
            return
        try:
            self._conduzir_eleicao()
        finally:
            with self._lock_eleicao:
                self._em_eleicao = False
                self._fim_eleicao.set()

    def _conduzir_eleicao(self) -> None:
        """Conduz uma eleição pelo Algoritmo do Valentão, do ponto de vista deste
        processo (seção 3.3 do enunciado).

        Requisitos (nível 3):
          * todos os processos maiores são desafiados;
          * ao final, ou este processo se tornou coordenador (era o maior vivo), ou
            recebeu o anúncio de um maior;
          * se um maior responder e depois cair antes de anunciar, a eleição não
            fica parada: ela recomeça;
          * o método só retorna quando a eleição tem um desfecho (ou o processo para).
        """
        while self.ativo:
            self._anuncio_recebido.clear()
            maiores = [pid for pid in sorted(config.PORTAS) if pid > self.meu_id]
            alvos_txt = ", ".join(f"P{p}" for p in maiores) or "nenhum"
            self._log(T.cor(f"eleição iniciada -> desafiando [{alvos_txt}]", "amarelo"))

            respostas: List[int] = []
            if maiores:
                lock_resp = threading.Lock()

                def desafiar(destino: int) -> None:
                    resp = self._enviar_e_aguardar(
                        destino, ELECTION, timeout=config.TIMEOUT_ELEICAO
                    )
                    if resp is not None and resp.tipo == ANSWER:
                        with lock_resp:
                            respostas.append(destino)

                threads = [
                    threading.Thread(target=desafiar, args=(pid,), daemon=True)
                    for pid in maiores
                ]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join()

            if not self.ativo:
                return

            if not respostas:
                if self._anuncio_recebido.is_set() and self.coordenador_id > self.meu_id:
                    self._log(
                        f"anúncio de P{self.coordenador_id} já recebido; encerrando eleição"
                    )
                    return
                self._tornar_coordenador()
                return

            quem_resp = ", ".join(f"P{p}" for p in sorted(respostas))
            self._log(f"ANSWER recebido de [{quem_resp}]; aguardando COORDINATOR")
            limite = time.monotonic() + 2 * config.TIMEOUT_ELEICAO
            while self.ativo:
                restante = limite - time.monotonic()
                if restante <= 0:
                    break
                if self._anuncio_recebido.wait(timeout=min(0.1, restante)):
                    return
            if not self.ativo:
                return
            self._log(T.cor("timeout aguardando COORDINATOR -> recomeçando eleição", "amarelo"))

    def _receber_election(self, origem: int) -> Optional[bytes]:
        """Chegou um ELECTION de ``origem`` (um processo menor está em eleição).

        Requisito (nível 3): cumprir a regra do Valentão para quem recebe um desafio
        (seção 3.3, regra 2). O valor de retorno são os bytes da resposta enviada
        na mesma conexão.
        """
        self._log(f"ELECTION de P{origem} -> respondo ANSWER e assumo a eleição")
        self._nova_thread(self.iniciar_eleicao, f"eleicao-resp-P{self.meu_id}")
        return montar(ANSWER, self.meu_id)

    def _tornar_coordenador(self) -> None:
        """Este processo venceu a eleição: assume (ou reafirma) a coordenação e avisa
        todos os outros.

        Requisitos:
          * um coordenador RECÉM-eleito não herda fila nem titular do anterior
            (esse estado morreu com ele);                                  (nível 3)
          * todos os demais processos ficam sabendo quem é o coordenador;  (nível 3)
          * [DESAFIO, n5_34] um coordenador que apenas se REAFIRMA (já era o
            coordenador) não perde a fila nem o titular;
          * [DESAFIO, n5_30] terminado o anúncio, o coordenador sabe se alguém
            entrou na RC autorizado pelo coordenador anterior (seção 3.4), e nenhum
            REQUEST é atendido antes que ele saiba.
        """
        # Mantemos _lock_rc durante os anúncios para que nenhum REQUEST novo seja
        # atendido antes de descobrirmos, pelos ACKs, se algum processo já está na RC.
        with self._lock_rc:
            if not self.ativo:
                return
            ja_era_coord = self.sou_coordenador()
            if not ja_era_coord:
                self.titular_rc = None
                self.fila_requests.clear()

            with self._lock_cliente:
                eu_na_rc = self.estado == NA_RC_ESTADO
            if eu_na_rc and self.titular_rc is None:
                self.titular_rc = self.meu_id

            self.coordenador_id = self.meu_id
            acao = "REAFIRMO COORDENAÇÃO" if ja_era_coord else "SOU O COORDENADOR"
            self._log(T.cor(f"=== {acao} (P{self.meu_id}) ===", "magenta_forte"))

            outros = [pid for pid in sorted(config.PORTAS) if pid != self.meu_id]
            lock_ack = threading.Lock()

            def anunciar(destino: int) -> None:
                resp = self._enviar_e_aguardar(destino, COORDINATOR)
                if resp is not None and resp.tipo == ACK and resp.extra == NA_RC:
                    with lock_ack:
                        if self.sou_coordenador() and self.titular_rc is None:
                            self.titular_rc = destino
                            self._log(
                                T.cor(
                                    f"P{destino} está na RC (autorizado pelo coordenador anterior)",
                                    "amarelo",
                                )
                            )

            threads = [
                threading.Thread(target=anunciar, args=(pid,), daemon=True)
                for pid in outros
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            if not self.sou_coordenador():
                self.titular_rc = None
                self.fila_requests.clear()

    def _receber_coordinator(self, novo: int) -> Optional[bytes]:
        """Chegou um COORDINATOR: ``novo`` anuncia que venceu a eleição.

        Requisitos:
          * a visão deste processo sobre quem é o coordenador é atualizada;  (nível 3)
          * quem estiver esperando o desfecho de uma eleição neste processo
            deixa de esperar;                                                 (nível 3)
          * se este processo era o coordenador, ele deixa de ser e descarta a fila;
          * a resposta é um ACK, que informa se este processo está DENTRO da RC
            (formato na tabela da seção 4);                     ([DESAFIO] n5_30)
          * [DESAFIO, n5_32] o anúncio de um processo MENOR que este, vivo, não é
            aceito: o Valentão manda contestá-lo.
        """
        if novo < self.meu_id:
            self._log(
                T.cor(
                    f"COORDINATOR de P{novo} (menor que P{self.meu_id}) contestado -> iniciando eleição",
                    "amarelo",
                )
            )
            self._nova_thread(self.iniciar_eleicao, f"eleicao-contesta-P{self.meu_id}")
            return None

        era_coord = self.sou_coordenador()
        self.coordenador_id = novo
        self._anuncio_recebido.set()

        if era_coord and novo != self.meu_id:
            if self._lock_rc.acquire(timeout=0.05):
                try:
                    if not self.sou_coordenador():
                        self.titular_rc = None
                        self.fila_requests.clear()
                finally:
                    self._lock_rc.release()

        with self._lock_cliente:
            na_rc = self.estado == NA_RC_ESTADO

        extra = NA_RC if na_rc else ""
        detalhe = " [enviando ACK com NA_RC]" if na_rc else ""
        self._log(T.cor(f"novo coordenador aceito: P{novo}{detalhe}", "magenta"))
        return montar(ACK, self.meu_id, extra)

    # ==================================================================
    # PARTE D -- Detecção de falhas (heartbeat)
    # ==================================================================

    def _heartbeat(self) -> None:
        """Vigia o coordenador durante toda a vida do processo (seção 3.2), em uma
        thread própria.

        Requisitos (níveis 3 e 4):
          * a queda do coordenador é percebida em no máximo cerca de
            INTERVALO_HEARTBEAT + TIMEOUT_RESPOSTA e leva a uma eleição;
          * sem falha, não há eleição (teste n3_24);
          * o processo não vigia a si mesmo quando é o coordenador;
          * a vigilância começa após um atraso aleatório (JITTER_INICIAL_MAX) e para
            prontamente quando o processo é encerrado.
        """
        if config.JITTER_INICIAL_MAX > 0:
            atraso = random.uniform(0.0, config.JITTER_INICIAL_MAX)
            if self._parado.wait(timeout=atraso):
                return

        while not self._parado.wait(timeout=config.INTERVALO_HEARTBEAT):
            coord = self.coordenador_id
            if coord == self.meu_id or self._em_eleicao:
                continue
            resp = self._enviar_e_aguardar(coord, PING)
            if not self.ativo:
                return
            if resp is None or resp.tipo != PONG:
                if self.coordenador_id == coord:
                    self._log(
                        T.cor(f"PING ao coordenador P{coord} sem resposta -> eleição", "amarelo")
                    )
                    self.iniciar_eleicao()

    # ==================================================================
    # Diagnóstico                                     (PRONTO, não mexer)
    # ==================================================================

    def status(self) -> str:
        with self._lock_rc:
            titular = f"P{self.titular_rc}" if self.titular_rc is not None else "(livre)"
            fila = self._fila_txt()
        linhas = [
            f"  processo     : P{self.meu_id}  (porta {self.minha_porta})",
            f"  coordenador  : P{self.coordenador_id}" + ("  <- EU" if self.sou_coordenador() else ""),
            f"  meu estado   : {self.estado}",
            f"  em eleição   : {'sim' if self._em_eleicao else 'não'}",
        ]
        if self.sou_coordenador():
            linhas += [f"  titular RC   : {titular}", f"  fila         : {fila}"]
        return "\n".join(linhas)


# =============================================================================
# Linha de comando                                        (PRONTO, não mexer)
# =============================================================================

AJUDA = """\
 Comandos:
   request [mensagem]   pede a região crítica (em segundo plano)
   status               mostra o estado deste processo
   eleicao              força uma eleição
   sair                 encerra o processo (simula uma FALHA, se for o coordenador)
   ajuda                mostra esta lista
 (Os logs chegam a qualquer momento; se o prompt "sumir", basta digitar.)"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Processo do sistema distribuído")
    parser.add_argument("id", type=int, help="ID do processo (ver config.PORTAS)")
    parser.add_argument("--coordenador", type=int, default=None,
                        help="coordenador presumido ao iniciar (padrão: maior ID)")
    parser.add_argument("--verbose", action="store_true", help="mostra também PING/PONG")
    parser.add_argument("--sem-cores", action="store_true", help="desliga as cores")
    args = parser.parse_args()

    if args.sem_cores:
        T.ativar_cores(False)
    Processo.verbose = args.verbose

    p = Processo(args.id, args.coordenador)
    try:
        p.iniciar()
    except OSError as e:
        print(f"Não foi possível abrir a porta {config.PORTAS[args.id]}: {e}")
        print("Já existe um processo com esse ID rodando?")
        return 1

    print(f"\n=== Processo P{args.id} pronto ===\n{AJUDA}\n")
    try:
        while True:
            try:
                linha = input(f"P{args.id}> ").strip()
            except EOFError:
                break
            comando, _, resto = linha.partition(" ")
            if not comando:
                continue
            if comando in ("sair", "exit", "quit"):
                break
            if comando == "status":
                print(p.status())
            elif comando == "eleicao":
                p._nova_thread(p.iniciar_eleicao, f"eleicao-cli-P{p.meu_id}")  # noqa: SLF001
            elif comando == "request":
                msg = resto.strip() or "trabalho"
                threading.Thread(target=p.requisitar_rc, args=(msg,), daemon=True).start()
            elif comando in ("ajuda", "help", "?"):
                print(AJUDA)
            else:
                print(f"comando desconhecido: {comando!r} (digite 'ajuda')")
    except KeyboardInterrupt:
        print()
    finally:
        p.parar()
    return 0


if __name__ == "__main__":
    sys.exit(main())
