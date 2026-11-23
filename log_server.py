"""
log_server.py
=============

Servidor gRPC de log -- PRONTO, não faz parte do que você implementa.

Cada processo chama ``RegistrarAcesso`` duas vezes por acesso à região
crítica (RC): ao ENTRAR e ao SAIR. Com isso o servidor funciona como um
ÁRBITRO externo e independente dos processos:

  * mostra, em ordem global, quem entrou/saiu da RC e quem autorizou;
  * mantém a ocupação atual da RC  ->  ``[RC: P2]`` ou ``[RC: livre]``;
  * destaca quando o coordenador que autoriza os acessos muda;
  * denuncia em VERMELHO qualquer VIOLAÇÃO de exclusão mútua (dois
    processos dentro da RC ao mesmo tempo);
  * imprime um resumo ao ser encerrado (Ctrl+C).

Exemplo de saída:

    #0001 10:41:07.120  ENTROU  P1  autorizado por P5  "operacao-um"     [RC: P1]
    #0002 10:41:15.125  SAIU    P1  ficou 8.00s                         [RC: livre]
    ==================== coordenador agora é P4 (antes: P5) ====================

Uso:
    python log_server.py [--porta 60051] [--sem-cores]
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
import threading
import time
from collections import Counter
from concurrent import futures
from typing import Dict, List, Optional

import grpc

import log_service_pb2 as pb
import log_service_pb2_grpc as pb_grpc
import terminal as T
from config import LOG_SERVER_PORTA


class LogServiceServicer(pb_grpc.LogServiceServicer):
    """Implementação do serviço de log.

    Também é usada pelos testes e pelas demonstrações (com ``imprimir=False``),
    que inspecionam os atributos abaixo:

    acessos     lista de dicts, um por ENTRADA, na ordem de chegada
                (process_id, coordenador_id, mensagem, timestamp, recebido_em)
    saidas      lista de dicts, um por SAIDA
    violacoes   lista de textos descrevendo violações de exclusão mútua
    """

    def __init__(self, imprimir: bool = True) -> None:
        self._lock = threading.Lock()
        self._imprimir = imprimir
        self.acessos: List[dict] = []
        self.saidas: List[dict] = []
        self.violacoes: List[str] = []
        self._dentro: Dict[int, float] = {}  # pid -> instante de entrada
        self._ultimo_coord: Optional[int] = None
        self._contador = 0

    # ------------------------------------------------------------------ RPC

    def RegistrarAcesso(self, request: pb.AcessoRequest, context):  # noqa: N802
        agora = time.monotonic()
        with self._lock:
            self._contador += 1
            n = self._contador
            pid, coord = request.process_id, request.coordenador_id
            registro = {
                "n": n,
                "process_id": pid,
                "coordenador_id": coord,
                "mensagem": request.mensagem,
                "timestamp": request.timestamp,
                "recebido_em": agora,
            }
            linhas: List[str] = []

            if request.evento == pb.SAIDA:
                inicio = self._dentro.pop(pid, None)
                registro["duracao"] = None if inicio is None else agora - inicio
                self.saidas.append(registro)
                linhas.append(self._linha_saida(registro))
            else:
                if coord != self._ultimo_coord and self._ultimo_coord is not None:
                    linhas.append(self._faixa_coordenador(coord, self._ultimo_coord))
                self._ultimo_coord = coord
                if self._dentro:
                    ocupantes = ", ".join(f"P{p}" for p in sorted(self._dentro))
                    texto = (
                        f"VIOLAÇÃO DE EXCLUSÃO MÚTUA: P{pid} entrou enquanto "
                        f"{ocupantes} ainda está(ão) na RC"
                    )
                    self.violacoes.append(texto)
                    linhas.append(T.cor(f"!!! {texto} !!!", "vermelho_forte"))
                self._dentro[pid] = agora
                self.acessos.append(registro)
                linhas.append(self._linha_entrada(registro))

        if self._imprimir:
            for linha in linhas:
                print(linha, flush=True)

        tipo = "saída" if request.evento == pb.SAIDA else "entrada"
        return pb.AcessoResponse(sucesso=True, confirmacao=f"{tipo} #{n} registrada")

    # ----------------------------------------------------------- formatação

    def _ocupacao(self) -> str:
        if not self._dentro:
            return T.cor("[RC: livre]", "cinza")
        nomes = ", ".join(f"P{p}" for p in sorted(self._dentro))
        estilo = "vermelho_forte" if len(self._dentro) > 1 else "negrito"
        return T.cor(f"[RC: {nomes}]", estilo)

    @staticmethod
    def _hora() -> str:
        return dt.datetime.now().strftime("%H:%M:%S.%f")[:-3]

    def _linha_entrada(self, r: dict) -> str:
        quem = T.rotulo_processo(r["process_id"])
        coord = T.rotulo_processo(r["coordenador_id"])
        msg = f'"{r["mensagem"]}"'
        return (
            f"#{r['n']:04d} {self._hora()}  {T.cor('ENTROU', 'verde')}  {quem}  "
            f"autorizado por {coord}  {msg:<22} {self._ocupacao()}"
        )

    def _linha_saida(self, r: dict) -> str:
        quem = T.rotulo_processo(r["process_id"])
        dur = "?" if r["duracao"] is None else f"{r['duracao']:.2f}s"
        return (
            f"#{r['n']:04d} {self._hora()}  {T.cor('SAIU  ', 'ciano')}  {quem}  "
            f"ficou {dur:<29} {self._ocupacao()}"
        )

    @staticmethod
    def _faixa_coordenador(novo: int, antigo: int) -> str:
        texto = f" coordenador agora é P{novo} (antes: P{antigo}) "
        return T.cor(texto.center(78, "="), "magenta")

    def resumo(self) -> str:
        """Resumo textual (impresso ao encerrar o servidor)."""
        with self._lock:
            por_proc = Counter(a["process_id"] for a in self.acessos)
            por_coord = Counter(a["coordenador_id"] for a in self.acessos)
            linhas = [
                "",
                "=" * 78,
                f"RESUMO: {len(self.acessos)} acesso(s) à RC",
                "  por processo   : "
                + (", ".join(f"P{p}={c}" for p, c in sorted(por_proc.items())) or "-"),
                "  por coordenador: "
                + (", ".join(f"P{p}={c}" for p, c in sorted(por_coord.items())) or "-"),
            ]
            if self.violacoes:
                linhas.append(T.cor(f"  VIOLAÇÕES: {len(self.violacoes)}", "vermelho_forte"))
            else:
                linhas.append(T.cor("  nenhuma violação de exclusão mútua detectada", "verde"))
            linhas.append("=" * 78)
        return "\n".join(linhas)


def criar_servidor(porta: int, servicer: LogServiceServicer) -> grpc.Server:
    """Cria (sem iniciar) um servidor gRPC com o servicer dado."""
    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pb_grpc.add_LogServiceServicer_to_server(servicer, servidor)
    servidor.add_insecure_port(f"127.0.0.1:{porta}")
    return servidor


def servir(porta: int) -> None:
    servicer = LogServiceServicer(imprimir=True)
    servidor = criar_servidor(porta, servicer)
    servidor.start()
    print(T.cor(f"[LOG-SERVER] escutando em 127.0.0.1:{porta}", "negrito"), flush=True)
    print("[LOG-SERVER] aguardando acessos à RC... (Ctrl+C para sair)\n", flush=True)
    try:
        servidor.wait_for_termination()
    except KeyboardInterrupt:
        print(servicer.resumo(), flush=True)
        servidor.stop(grace=1.0)


def main() -> int:
    parser = argparse.ArgumentParser(description="Servidor gRPC de log (árbitro da RC)")
    parser.add_argument("--porta", type=int, default=LOG_SERVER_PORTA)
    parser.add_argument("--sem-cores", action="store_true", help="desliga as cores")
    args = parser.parse_args()
    if args.sem_cores:
        T.ativar_cores(False)
    servir(args.porta)
    return 0


if __name__ == "__main__":
    sys.exit(main())
