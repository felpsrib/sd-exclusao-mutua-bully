"""
visualizacao.py
===============

Ferramentas para VER o que aconteceu numa execução (PRONTO, não mexer).

Quando vários processos rodam no mesmo programa Python (testes e
``cenarios.py``), um ``Rastreador`` instalado em ``Processo.observador``
recebe todos os eventos: mensagens trocadas, entradas/saídas da RC,
mudanças de coordenador, falhas. A partir deles este módulo produz:

  * ``diagrama_texto(...)``  -> diagrama de sequência no terminal, com uma
                                "raia" vertical por processo;
  * ``gerar_html(...)``      -> página HTML autocontida com (1) a linha do
                                tempo da RC (quem esteve dentro, quando e
                                autorizado por quem), (2) o diagrama de
                                sequência completo e (3) a tabela de acessos.

Legenda das raias (terminal):

    │  processo vivo         ║  processo que se considera coordenador
    ┃  processo DENTRO da RC (espessura = exclusão mútua visível)
    ◆  processo iniciado     ✖  processo encerrado (falha)
    ★  assumiu a coordenação
    ●──REQUEST──▶  mensagem entregue     ●╌╌ELECTION╌╌✕  destino inalcançável
"""

from __future__ import annotations

import html
import threading
import time
from typing import Dict, Iterable, List, Optional, Sequence

import terminal as T

# =============================================================================
# Coleta
# =============================================================================


class Rastreador:
    """Coleta eventos dos processos. Use como gancho de ``Processo.observador``.

    >>> r = Rastreador().instalar()
    >>> ...  # sobe processos, executa o cenário
    >>> print(diagrama_texto(r.eventos))
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.eventos: List[dict] = []

    def __call__(self, evento: dict) -> None:
        with self._lock:
            self.eventos.append(evento)

    def instalar(self) -> "Rastreador":
        from processo import Processo

        Processo.observador = self
        return self

    def remover(self) -> None:
        from processo import Processo

        if Processo.observador is self:
            Processo.observador = None

    def marcar(self, texto: str) -> None:
        """Insere um marco (ex.: 'FASE 3 - P5 falha') na linha do tempo."""
        self({"t": time.time(), "ev": "nota", "pid": 0, "texto": texto})

    def copia(self) -> List[dict]:
        with self._lock:
            return sorted(self.eventos, key=lambda e: e["t"])


# =============================================================================
# Pré-processamento
# =============================================================================


def _relevante(ev: dict, completo: bool) -> bool:
    """Filtra o ruído: heartbeats e ACKs vazios só aparecem com completo=True."""
    if ev["ev"] == "msg":
        if completo:
            return True
        if ev["tipo"] in ("PING", "PONG"):
            return not ev.get("ok", True)  # PING que falhou é informativo
        if ev["tipo"] == "ACK":
            return bool(ev.get("extra"))
        return True
    if ev["ev"] == "coord":
        return ev["coord"] == ev["pid"]  # só "Pk assumiu", não cada mudança de visão
    return ev["ev"] in ("inicio", "parada", "rc_entrada", "rc_saida", "nota")


def _pids(eventos: Iterable[dict]) -> List[int]:
    vistos = set()
    for e in eventos:
        if e.get("pid"):
            vistos.add(e["pid"])
        for chave in ("de", "para"):
            if e.get(chave):
                vistos.add(e[chave])
    return sorted(vistos)


class _Raias:
    """Acompanha, evento a evento, o estado visual de cada raia."""

    def __init__(self) -> None:
        self.vivo: Dict[int, bool] = {}
        self.na_rc: Dict[int, Optional[int]] = {}  # pid -> coordenador que autorizou
        self.coord: Dict[int, bool] = {}

    def aplicar(self, ev: dict) -> None:
        pid, tipo = ev["pid"], ev["ev"]
        if tipo == "inicio":
            self.vivo[pid] = True
            self.coord[pid] = ev.get("coord") == pid
        elif tipo == "parada":
            self.vivo[pid] = False
            self.coord[pid] = False
            self.na_rc[pid] = None
        elif tipo == "coord":
            self.coord[pid] = ev["coord"] == pid
        elif tipo == "rc_entrada":
            self.na_rc[pid] = ev.get("coord")
        elif tipo == "rc_saida":
            self.na_rc[pid] = None


def _descrever(ev: dict) -> str:
    tipo = ev["ev"]
    if tipo == "inicio":
        return f"P{ev['pid']} iniciado (coordenador presumido: P{ev.get('coord')})"
    if tipo == "parada":
        return f"P{ev['pid']} encerrado (falha)"
    if tipo == "coord":
        return f"P{ev['pid']} assume a coordenação"
    if tipo == "rc_entrada":
        return f"P{ev['pid']} ENTRA na RC (autorizado por P{ev.get('coord')})"
    if tipo == "rc_saida":
        if ev.get("falhou"):
            return f"P{ev['pid']} caiu DENTRO da RC"
        return f"P{ev['pid']} SAI da RC"
    if tipo == "msg":
        extra = f"[{ev['extra']}]" if ev.get("extra") else ""
        destino = "" if ev.get("ok", True) else " (inalcançável)"
        return f"P{ev['de']} -> P{ev['para']}: {ev['tipo']}{extra}{destino}"
    return ev.get("texto", "")


# =============================================================================
# Diagrama no terminal
# =============================================================================

_UNI = {"vivo": "│", "coord": "║", "rc": "┃", "ini": "◆", "fim": "✖", "star": "★",
        "src": "●", "dir": "▶", "esq": "◀", "x": "✕", "ok": "─", "falha": "╌",
        "ent": "┳", "sai": "┻", "sep": "─"}
_ASC = {"vivo": "|", "coord": "!", "rc": "#", "ini": "+", "fim": "X", "star": "*",
        "src": "o", "dir": ">", "esq": "<", "x": "x", "ok": "-", "falha": ".",
        "ent": "v", "sai": "^", "sep": "-"}


_ABREV = {"COORDINATOR": "COORD", "ELECTION": "ELECT", "REQUEST": "REQ", "RELEASE": "REL"}


def diagrama_texto(
    eventos: Sequence[dict],
    pids: Optional[Sequence[int]] = None,
    completo: bool = False,
    largura: int = 13,
    ascii_puro: bool = False,
) -> str:
    """Desenha um diagrama de sequência com uma raia por processo.

    completo=True inclui PING/PONG e ACKs vazios (fica bem mais longo).
    """
    s = _ASC if ascii_puro else _UNI
    evs = sorted(eventos, key=lambda e: e["t"])
    if not evs:
        return "(nenhum evento)"
    pids = list(pids) if pids else _pids(evs)
    idx = {p: i for i, p in enumerate(pids)}
    t0 = evs[0]["t"]
    W = largura
    centro = [i * W + W // 2 for i in range(len(pids))]
    raias = _Raias()

    cab = "".join(f"P{p}".center(W) for p in pids)
    linhas = [" " * 10 + T.cor(cab, "negrito"), " " * 10 + "".join("┬".center(W) if not ascii_puro else "+".center(W) for _ in pids)]

    for ev in evs:
        raias.aplicar(ev)
        if not _relevante(ev, completo):
            continue
        if ev["ev"] == "nota":
            texto = f" {ev['texto']} "
            linhas.append(" " * 10 + T.cor(texto.center(W * len(pids), s["sep"]), "negrito"))
            continue

        row = []
        for p in pids:
            if not raias.vivo.get(p) and not (ev["ev"] == "parada" and ev["pid"] == p):
                row.append(" ")
            elif raias.na_rc.get(p) is not None:
                row.append(s["rc"])
            elif raias.coord.get(p):
                row.append(s["coord"])
            else:
                row.append(s["vivo"])
        chars = [" "] * (W * len(pids))
        for i, c in enumerate(row):
            chars[centro[i]] = c
        cores: Dict[int, str] = {}
        nota = ""

        if ev["ev"] == "msg" and ev["de"] in idx and ev["para"] in idx:
            a, b = centro[idx[ev["de"]]], centro[idx[ev["para"]]]
            ok = ev.get("ok", True)
            passo = 1 if b > a else -1
            for x in range(a + passo, b, passo):
                chars[x] = s["ok"] if ok else s["falha"]
            chars[a] = s["src"]
            chars[b] = (s["dir"] if passo > 0 else s["esq"]) if ok else s["x"]
            rotulo = ev["tipo"] + (f"[{ev['extra']}]" if ev.get("extra") else "")
            vao = abs(b - a) - 3
            if len(rotulo) > vao:
                rotulo = _ABREV.get(ev["tipo"], ev["tipo"])
            if len(rotulo) > vao:
                rotulo = rotulo[: max(vao, 3)]
            ini = min(a, b) + 2 + (vao - len(rotulo)) // 2
            for k, ch in enumerate(rotulo):
                chars[ini + k] = ch
            cor_msg = T.COR_MENSAGEM.get(ev["tipo"]) if ok else "vermelho"
            for x in range(min(a, b), max(a, b) + 1):
                cores[x] = cor_msg
        else:
            c = centro[idx[ev["pid"]]]
            simbolo = {"inicio": s["ini"], "parada": s["fim"], "coord": s["star"],
                       "rc_entrada": s["ent"], "rc_saida": s["sai"]}.get(ev["ev"], "?")
            if ev["ev"] == "rc_saida" and ev.get("falhou"):
                simbolo = s["fim"]
            chars[c] = simbolo
            cores[c] = {"parada": "vermelho", "coord": "magenta", "rc_entrada": "verde",
                        "rc_saida": "verde"}.get(ev["ev"], "negrito")
            nota = _descrever(ev)

        # Pinta (se as cores estiverem ativas) e monta a linha.
        texto = "".join(T.cor(ch, cores[x]) if x in cores else
                        (T.cor(ch, "verde") if ch == s["rc"] else ch)
                        for x, ch in enumerate(chars))
        tempo = f"{ev['t'] - t0:8.3f}s "
        linhas.append(T.cor(tempo, "cinza") + " " + texto + ("  " + nota if nota else ""))
    return "\n".join(linhas)


# =============================================================================
# HTML (linha do tempo da RC + diagrama de sequência + tabela)
# =============================================================================

# Cores por tipo de mensagem: paleta categórica validada (claro / escuro).
_COR_MSG = {
    "REQUEST": ("#2a78d6", "#3987e5"),
    "GRANT": ("#008300", "#2f9e2f"),
    "RELEASE": ("#1baf7a", "#199e70"),
    "ELECTION": ("#eb6834", "#d95926"),
    "ANSWER": ("#eda100", "#c98500"),
    "COORDINATOR": ("#6250d6", "#9085e9"),
    "ACK": ("#e87ba4", "#d55181"),
    "PING": ("#8a8984", "#77766f"),
    "PONG": ("#8a8984", "#77766f"),
}
# Cores das barras da RC, por coordenador que autorizou (ordem de aparição).
_COR_COORD = [("#2a78d6", "#3987e5"), ("#eb6834", "#d95926"), ("#1baf7a", "#199e70"),
              ("#6250d6", "#9085e9"), ("#e87ba4", "#d55181")]


def _acessos(evs: Sequence[dict]) -> List[dict]:
    """Pareia rc_entrada/rc_saida em acessos {pid, coord, ini, fim, mensagem}."""
    abertos: Dict[int, dict] = {}
    lista: List[dict] = []
    for e in evs:
        if e["ev"] == "rc_entrada":
            a = {"pid": e["pid"], "coord": e.get("coord"), "ini": e["t"], "fim": None,
                 "mensagem": e.get("mensagem", ""), "falhou": False}
            abertos[e["pid"]] = a
            lista.append(a)
        elif e["ev"] == "rc_saida" and e["pid"] in abertos:
            a = abertos.pop(e["pid"])
            a["fim"] = e["t"]
            a["falhou"] = bool(e.get("falhou"))
    return lista


def _intervalos_coord(evs: Sequence[dict], t_fim: float) -> List[dict]:
    """Períodos em que cada processo se considerou coordenador."""
    ini: Dict[int, float] = {}
    out: List[dict] = []
    for e in evs:
        pid = e["pid"]
        sou = None
        if e["ev"] == "inicio":
            sou = e.get("coord") == pid
        elif e["ev"] == "coord":
            sou = e["coord"] == pid
        elif e["ev"] == "parada":
            sou = False
        if sou is True and pid not in ini:
            ini[pid] = e["t"]
        elif sou is False and pid in ini:
            out.append({"pid": pid, "ini": ini.pop(pid), "fim": e["t"]})
    for pid, t in ini.items():
        out.append({"pid": pid, "ini": t, "fim": t_fim})
    return out


def _passo_eixo(duracao: float) -> float:
    for passo in (0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 30, 60):
        if duracao / passo <= 12:
            return passo
    return 120


def _svg_linha_do_tempo(evs, pids, t0, t1, cor_coord) -> str:
    esq, larg, alt_linha, topo = 56, 880, 34, 26
    W = esq + larg + 20
    H = topo + alt_linha * len(pids) + 34
    dur = max(t1 - t0, 1e-6)

    def x(t: float) -> float:
        return esq + (t - t0) / dur * larg

    p = [f'<svg viewBox="0 0 {W} {H}" class="gantt" role="img" '
         f'aria-label="Linha do tempo da região crítica">']
    passo = _passo_eixo(dur)
    k = 0.0
    while k <= dur + 1e-9:
        xx = x(t0 + k)
        p.append(f'<line x1="{xx:.1f}" y1="{topo - 6}" x2="{xx:.1f}" y2="{H - 28}" class="grade"/>')
        p.append(f'<text x="{xx:.1f}" y="{H - 12}" class="eixo" text-anchor="middle">{k:g}s</text>')
        k += passo
    for i, pid in enumerate(pids):
        y = topo + i * alt_linha
        p.append(f'<text x="{esq - 12}" y="{y + alt_linha / 2 + 4:.1f}" class="rot" text-anchor="end">P{pid}</text>')
        p.append(f'<line x1="{esq}" y1="{y + alt_linha - 1}" x2="{esq + larg}" y2="{y + alt_linha - 1}" class="base"/>')
    for c in _intervalos_coord(evs, t1):
        if c["pid"] not in pids:
            continue
        y = topo + pids.index(c["pid"]) * alt_linha
        x0, x1 = x(c["ini"]), x(c["fim"])
        p.append(f'<rect x="{x0:.1f}" y="{y + alt_linha - 7}" width="{max(x1 - x0, 2):.1f}" height="4" '
                 f'rx="2" class="faixa-coord"><title>P{c["pid"]} coordenador de '
                 f'{c["ini"] - t0:.2f}s a {c["fim"] - t0:.2f}s</title></rect>')
    for a in _acessos(evs):
        if a["pid"] not in pids:
            continue
        y = topo + pids.index(a["pid"]) * alt_linha
        fim = a["fim"] if a["fim"] is not None else t1
        x0, x1 = x(a["ini"]), x(fim)
        w = max(x1 - x0, 3)
        classe = f'rc c{cor_coord.get(a["coord"], 0)}'
        dica = (f'P{a["pid"]} na RC de {a["ini"] - t0:.2f}s a {fim - t0:.2f}s '
                f'({fim - a["ini"]:.2f}s), autorizado por P{a["coord"]}')
        p.append(f'<rect x="{x0:.1f}" y="{y + 5}" width="{w:.1f}" height="{alt_linha - 15}" rx="4" '
                 f'class="{classe}"><title>{html.escape(dica)}</title></rect>')
        if w > 34:
            p.append(f'<text x="{x0 + w / 2:.1f}" y="{y + alt_linha / 2 + 1:.1f}" class="rot-barra" '
                     f'text-anchor="middle">P{a["coord"]}</text>')
    for e in evs:
        if e["ev"] == "parada" and e["pid"] in pids:
            y = topo + pids.index(e["pid"]) * alt_linha + alt_linha / 2 - 2
            xx = x(e["t"])
            p.append(f'<g class="falha"><title>P{e["pid"]} falhou em {e["t"] - t0:.2f}s</title>'
                     f'<path d="M{xx - 6:.1f},{y - 6:.1f} l12,12 M{xx + 6:.1f},{y - 6:.1f} l-12,12"/></g>')
        elif e["ev"] == "nota":
            xx = x(e["t"])
            p.append(f'<line x1="{xx:.1f}" y1="{topo - 14}" x2="{xx:.1f}" y2="{H - 28}" class="marco"/>')
    p.append("</svg>")
    return "\n".join(p)


def _svg_sequencia(evs, pids, t0) -> str:
    rel = [e for e in evs if _relevante(e, False)]
    col, esq, topo, passo = 132, 92, 44, 24
    W = esq + col * len(pids) + 270  # espaço à direita para as descrições
    H = topo + passo * (len(rel) + 1) + 10
    cx = {p: esq + col * i + col / 2 for i, p in enumerate(pids)}

    # Intervalos (em índice de linha) para desenhar as raias.
    raias = _Raias()
    estilo_raia: Dict[int, List[str]] = {p: [] for p in pids}
    for e in rel:
        raias.aplicar(e)
        for p in pids:
            if not raias.vivo.get(p):
                estilo_raia[p].append("morto")
            elif raias.na_rc.get(p) is not None:
                estilo_raia[p].append("rc")
            elif raias.coord.get(p):
                estilo_raia[p].append("coord")
            else:
                estilo_raia[p].append("vivo")

    s = [f'<svg viewBox="0 0 {W} {H}" class="seq" role="img" aria-label="Diagrama de sequência">',
         "<defs>"]
    for tipo in _COR_MSG:
        s.append(f'<marker id="m-{tipo}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" '
                 f'markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" '
                 f'class="ponta t-{tipo}"/></marker>')
    s.append("</defs>")
    for p in pids:
        s.append(f'<text x="{cx[p]:.0f}" y="22" class="cab" text-anchor="middle">P{p}</text>')
        estilos = estilo_raia[p]
        k = 0
        while k < len(estilos):
            j = k
            while j + 1 < len(estilos) and estilos[j + 1] == estilos[k]:
                j += 1
            y0 = topo + passo * k - passo / 2
            y1 = topo + passo * j + passo / 2
            est = estilos[k]
            if est == "rc":
                s.append(f'<rect x="{cx[p] - 5:.0f}" y="{y0:.0f}" width="10" height="{y1 - y0:.0f}" rx="4" class="raia-rc"/>')
            elif est == "coord":
                s.append(f'<line x1="{cx[p]:.0f}" y1="{y0:.0f}" x2="{cx[p]:.0f}" y2="{y1:.0f}" class="raia-coord"/>')
            elif est == "vivo":
                s.append(f'<line x1="{cx[p]:.0f}" y1="{y0:.0f}" x2="{cx[p]:.0f}" y2="{y1:.0f}" class="raia"/>')
            k = j + 1

    for n, e in enumerate(rel):
        y = topo + passo * n
        s.append(f'<text x="8" y="{y + 4}" class="tempo">{e["t"] - t0:7.3f}s</text>')
        if e["ev"] == "nota":
            s.append(f'<line x1="{esq - 10}" y1="{y}" x2="{W - 4}" y2="{y}" class="marco"/>')
            s.append(f'<text x="{esq}" y="{y - 5}" class="nota">{html.escape(e["texto"])}</text>')
            continue
        if e["ev"] == "msg" and e["de"] in cx and e["para"] in cx:
            x0, x1 = cx[e["de"]], cx[e["para"]]
            ok = e.get("ok", True)
            d = 1 if x1 > x0 else -1
            fim = x1 - d * 7 if ok else x1 - d * 12
            classe = f't-{e["tipo"]}' + ("" if ok else " falhou")
            marcador = f' marker-end="url(#m-{e["tipo"]})"' if ok else ""
            dica = html.escape(_descrever(e))
            s.append(f'<g><title>{dica}</title><line x1="{x0 + d * 6:.0f}" y1="{y}" x2="{fim:.0f}" y2="{y}" '
                     f'class="seta {classe}"{marcador}/>')
            if not ok:
                s.append(f'<path d="M{x1 - 5:.0f},{y - 5} l10,10 M{x1 + 5:.0f},{y - 5} l-10,10" class="xis"/>')
            rot = e["tipo"] + (f" [{e['extra']}]" if e.get("extra") else "")
            s.append(f'<text x="{(x0 + x1) / 2:.0f}" y="{y - 4}" class="rot-msg" text-anchor="middle">'
                     f'{html.escape(rot)}</text></g>')
        else:
            if e["pid"] not in cx:
                continue
            x0 = cx[e["pid"]]
            simb = {"inicio": "◆", "parada": "✖", "coord": "★", "rc_entrada": "▼", "rc_saida": "▲"}[e["ev"]]
            classe = {"parada": "ev-falha", "coord": "ev-coord"}.get(e["ev"], "ev")
            s.append(f'<text x="{x0:.0f}" y="{y + 5}" class="{classe}" text-anchor="middle">{simb}</text>')
            s.append(f'<text x="{x0 + 12:.0f}" y="{y + 4}" class="desc">{html.escape(_descrever(e))}</text>')
    s.append("</svg>")
    return "\n".join(s)


def _css() -> str:
    claro = "\n".join(f"  --m-{t}: {c[0]};" for t, c in _COR_MSG.items())
    escuro = "\n".join(f"  --m-{t}: {c[1]};" for t, c in _COR_MSG.items())
    cc = "\n".join(f"  --c{i}: {c[0]};" for i, c in enumerate(_COR_COORD))
    ce = "\n".join(f"  --c{i}: {c[1]};" for i, c in enumerate(_COR_COORD))
    tipos = "\n".join(
        f".t-{t} {{ stroke: var(--m-{t}); }} .ponta.t-{t} {{ fill: var(--m-{t}); stroke: none; }}"
        for t in _COR_MSG)
    barras = "\n".join(f".rc.c{i} {{ fill: var(--c{i}); }} .leg-c{i} {{ background: var(--c{i}); }}"
                       for i in range(len(_COR_COORD)))
    tema_escuro = f"""  color-scheme: dark;
  --bg: #121211; --surface: #1a1a19; --texto: #f0efec; --texto2: #c3c2b7; --mudo: #8a897f;
  --grade: #2e2d2a; --raia: #5f5e58; --rc: #2f9e2f; --coord: #9085e9; --falha: #e66767;
{escuro}
{ce}"""
    return f"""
:root {{
  color-scheme: light;
  --bg: #f4f3ef; --surface: #fcfcfb; --texto: #0b0b0b; --texto2: #52514e; --mudo: #8a8984;
  --grade: #e6e5e0; --raia: #b9b8b2; --rc: #008300; --coord: #6250d6; --falha: #e34948;
{claro}
{cc}
}}
@media (prefers-color-scheme: dark) {{ :root:not([data-theme="light"]) {{
{tema_escuro}
}} }}
:root[data-theme="dark"] {{
{tema_escuro}
}}
* {{ box-sizing: border-box; }}
body {{ margin: 0; background: var(--bg); color: var(--texto);
  font: 15px/1.5 system-ui, -apple-system, "Segoe UI", Roboto, sans-serif; }}
main {{ max-width: 1120px; margin: 0 auto; padding: 28px 16px 60px; }}
h1 {{ font-size: 1.6rem; margin: 0 0 4px; }}
h2 {{ font-size: 1.1rem; margin: 0 0 6px; }}
.sub {{ color: var(--texto2); margin: 0 0 22px; max-width: 75ch; }}
section {{ background: var(--surface); border-radius: 12px; padding: 18px 18px 14px;
  margin-bottom: 18px; box-shadow: 0 1px 2px rgb(0 0 0 / .06); }}
section > p {{ color: var(--texto2); margin: 0 0 12px; font-size: .92rem; max-width: 80ch; }}
.rolagem {{ overflow-x: auto; }}
svg {{ display: block; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }}
svg.gantt {{ width: 100%; min-width: 640px; }}
svg.seq {{ min-width: 100%; }}
.grade {{ stroke: var(--grade); }}
.base {{ stroke: var(--grade); }}
.eixo, .tempo {{ fill: var(--mudo); font-size: 11px; }}
.rot, .cab {{ fill: var(--texto); font-size: 13px; font-weight: 600; }}
.rot-barra {{ fill: #fff; font-size: 11px; font-weight: 600; pointer-events: none; }}
.faixa-coord {{ fill: var(--coord); opacity: .55; }}
.falha path, .xis {{ stroke: var(--falha); stroke-width: 2.5; stroke-linecap: round; fill: none; }}
.marco {{ stroke: var(--texto2); stroke-dasharray: 3 4; }}
.raia {{ stroke: var(--raia); stroke-width: 1.5; }}
.raia-coord {{ stroke: var(--coord); stroke-width: 3.5; }}
.raia-rc {{ fill: var(--rc); }}
.seta {{ stroke-width: 2; fill: none; }}
.seta.falhou {{ stroke-dasharray: 5 4; opacity: .75; }}
.rot-msg {{ fill: var(--texto2); font-size: 10.5px; }}
.desc {{ fill: var(--texto2); font-size: 11px; }}
.ev {{ fill: var(--texto); font-size: 13px; }}
.ev-falha {{ fill: var(--falha); font-size: 14px; }}
.ev-coord {{ fill: var(--coord); font-size: 15px; }}
.nota {{ fill: var(--texto); font-size: 12px; font-weight: 700; font-family: system-ui, sans-serif; }}
{tipos}
{barras}
.legenda {{ display: flex; flex-wrap: wrap; gap: 6px 18px; font-size: .85rem; color: var(--texto2);
  margin: 10px 0 0; padding: 0; list-style: none; }}
.legenda li {{ display: flex; align-items: center; gap: 6px; }}
.amostra {{ width: 16px; height: 10px; border-radius: 3px; display: inline-block; }}
.traco {{ width: 20px; height: 0; border-top: 2px solid; display: inline-block; }}
table {{ border-collapse: collapse; width: 100%; font-size: .9rem; }}
th, td {{ text-align: left; padding: 6px 10px; border-bottom: 1px solid var(--grade); }}
th {{ color: var(--texto2); font-weight: 600; }}
td.num {{ font-variant-numeric: tabular-nums; }}
.veredito {{ font-weight: 600; }}
.veredito.ok {{ color: var(--rc); }}
.veredito.erro {{ color: var(--falha); }}
"""


def gerar_html(eventos: Sequence[dict], caminho: str, titulo: str, descricao: str = "") -> str:
    """Gera uma página HTML autocontida com a visualização da execução."""
    evs = sorted(eventos, key=lambda e: e["t"])
    if not evs:
        raise ValueError("nenhum evento para desenhar")
    pids = [p for p in _pids(evs) if p]
    t0, t1 = evs[0]["t"], evs[-1]["t"]
    acessos = _acessos(evs)
    ordem_coord: Dict[int, int] = {}
    for a in acessos:
        if a["coord"] not in ordem_coord:
            ordem_coord[a["coord"]] = len(ordem_coord) % len(_COR_COORD)

    # Verificação de exclusão mútua (sobreposição de intervalos).
    sobrepostos = []
    for i, a in enumerate(acessos):
        for b in acessos[i + 1:]:
            fa = a["fim"] if a["fim"] is not None else t1
            fb = b["fim"] if b["fim"] is not None else t1
            if a["ini"] < fb and b["ini"] < fa:
                sobrepostos.append((a, b))
    if sobrepostos:
        veredito = (f'<p class="veredito erro">✖ {len(sobrepostos)} sobreposição(ões) na RC: '
                    "a exclusão mútua foi violada.</p>")
    else:
        veredito = ('<p class="veredito ok">✔ Nenhuma sobreposição: no máximo um processo '
                    "esteve na RC em cada instante.</p>")

    leg_coord = "".join(
        f'<li><span class="amostra leg-c{c}"></span>autorizado por P{pid}</li>'
        for pid, c in ordem_coord.items())
    leg_msg = "".join(
        f'<li><span class="traco" style="border-color: var(--m-{t})"></span>{t}</li>'
        for t in _COR_MSG if t not in ("PING", "PONG"))

    linhas = []
    for i, a in enumerate(acessos, 1):
        if a["fim"] is None:
            saida = duracao = "—"
        else:
            saida = "%.3fs" % (a["fim"] - t0)
            duracao = "%.2fs" % (a["fim"] - a["ini"])
        if a["falhou"]:
            duracao += " (caiu na RC)"
        linhas.append(
            "<tr><td class='num'>%d</td><td>P%s</td><td>P%s</td><td>%s</td>"
            "<td class='num'>%.3fs</td><td class='num'>%s</td><td class='num'>%s</td></tr>"
            % (i, a["pid"], a["coord"], html.escape(a["mensagem"]), a["ini"] - t0, saida, duracao))
    linhas_tab = "".join(linhas)

    marcos = "".join(f"<li><b>{e['t'] - t0:.2f}s</b> — {html.escape(e['texto'])}</li>"
                     for e in evs if e["ev"] == "nota")

    pagina = f"""<!doctype html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(titulo)}</title>
<style>{_css()}</style>
</head>
<body>
<main>
<h1>{html.escape(titulo)}</h1>
<p class="sub">{html.escape(descricao)}</p>

<section>
<h2>1. Quem esteve na região crítica, e quando</h2>
<p>Cada barra é um acesso à RC; a cor indica o coordenador que o autorizou. A faixa fina
sob a linha mostra o período em que o processo se considerou coordenador; ✕ marca uma falha.
Passe o mouse sobre as barras para ver os tempos.</p>
{veredito}
<div class="rolagem">{_svg_linha_do_tempo(evs, pids, t0, t1, ordem_coord)}</div>
<ul class="legenda">{leg_coord}<li><span class="amostra" style="background: var(--coord); height: 4px"></span>coordenador</li></ul>
{f"<ul class='legenda' style='display:block'>{marcos}</ul>" if marcos else ""}
</section>

<section>
<h2>2. Diagrama de sequência</h2>
<p>O tempo corre para baixo; cada linha é um evento (o espaçamento não é proporcional ao tempo).
Raia grossa verde = processo dentro da RC; raia roxa = processo que se considera coordenador.
Setas tracejadas com ✕ são mensagens que não chegaram (destino caído). PING/PONG foram omitidos.</p>
<div class="rolagem">{_svg_sequencia(evs, pids, t0)}</div>
<ul class="legenda">{leg_msg}</ul>
</section>

<section>
<h2>3. Acessos à região crítica</h2>
<div class="rolagem"><table>
<thead><tr><th>#</th><th>Processo</th><th>Autorizado por</th><th>Mensagem</th><th>Entrada</th><th>Saída</th><th>Duração</th></tr></thead>
<tbody>{linhas_tab}</tbody></table></div>
</section>
</main>
</body>
</html>
"""
    with open(caminho, "w", encoding="utf-8") as f:
        f.write(pagina)
    return caminho
