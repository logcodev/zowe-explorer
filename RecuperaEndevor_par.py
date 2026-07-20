#!/usr/bin/env python3
"""
RecuperaEndevor_par.py - Inventario de elementos Endevor via Zowe CLI -> CSV
                         VERSION PARALELA: N consultas concurrentes.

Autor   : Albert Tapia
Version : 10.0

El input es una lista de NOMBRES DE ELEMENTO. El TYPE es un dato a
descubrir, no un filtro: se consulta con --typ * y se reporta lo que venga.
Un mismo elemento puede existir bajo varios types -> varias filas.
Los elementos que no existen salen con STATUS=NOT_FOUND.

Al arrancar pregunta por el ambiente:
    Produccion    -> BCPPROD  stage 2
    Certificacion -> BCPDCCAL stage 2
    Desarrollo    -> BCPDESA  stage 1
    Todos         -> sin filtro
El prompt sale por stderr, asi que puedes responderlo con un pipe:
    echo 2 | python RecuperaEndevor_par.py lista.txt

Siempre escribe un .csv; nada de volcar el CSV a la terminal.

Uso:
    RecuperaEndevor_par.py <archivo_input> [salida.csv]  -> lista_<fecha>.csv
    RecuperaEndevor_par.py <ELEMENT>            -> ELEMENT_<fecha>.csv

Variables de entorno opcionales:
    ENDEVOR_TYPE            (default: *)   filtro de type, si lo conoces
    ENDEVOR_LIMIT           (default: no se manda; el host esta en API v1)
    ENDEVOR_INSTANCE        (default: ENDEVOR)
    ENDEVOR_SYSTEM          (default: *)
    ENDEVOR_SUBSYSTEM       (default: *)
    ENDEVOR_CSV_DELIM       (default: ;)
    ENDEVOR_TIMEOUT         (default: 300) segundos por consulta
    ENDEVOR_WORKERS         (default: 4)   consultas concurrentes

OJO: cada worker es una sesion concurrente contra Endevor Web Services.
Subir WORKERS es una decision sobre carga en un host compartido, no una
optimizacion local. Consultalo antes de pasarte de 4.

Codigos de salida:
    0 ok | 1 uso | 2 input | 4 fallos parciales | 8 fallaron todas
"""

from __future__ import annotations

import csv
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from collections import OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from os.path import abspath, basename, isfile, splitext

# El JSON de --rfj trae decenas de campos por elemento; estos son los que
# van al CSV. (clave JSON, columna). Editar aqui es editar todo.
# OJO: stgNum es el stage real (1|2). stgId es un id interno y devuelve
# cosas como "4" -- el script original mapeaba ese por error.
FIELD_MAP = [
    ("elmName",       "ELEMENT"),
    ("typeName",      "TYPE"),
    ("envName",       "ENV"),
    ("stgNum",        "STAGE"),
    ("sysName",       "SYSTEM"),
    ("sbsName",       "SUBSYSTEM"),
    ("procGrpName",   "PROCGROUP"),
    ("signoutId",     "SIGNOUT"),
    ("lastActCcid",   "CCID"),
    ("lastActUserid", "USERID"),
    ("lastAct",       "ACCION"),
    ("lastActDate",   "DATETIME"),
]

KEYS = [k for k, _ in FIELD_MAP]
COLS = [c for _, c in FIELD_MAP] + ["STATUS"]

# Campos que Endevor devuelve en ISO y Excel no interpreta bien.
DATE_KEYS = {"lastActDate"}

# Ambientes Endevor de BCP: (etiqueta, --env, --sn). "" = no se manda el
# flag. El stage va atado al ambiente segun el mapa del site.
ENVIRONMENTS = [
    ("Todos",          "",         ""),
    ("Produccion",     "BCPPROD",  "2"),
    ("Certificacion",  "BCPDCCAL", "2"),
    ("Desarrollo",     "BCPDESA",  "1"),
]
ISO_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})")

# Charset de FIELD_RE de la extension VSCode, menos los comodines (* %):
# aqui cada nombre se cruza 1:1 contra la respuesta, y una mascara no
# tendria contra que cruzarse.
FIELD_RE = re.compile(r"^[A-Za-z0-9$#@._-]+$")


PRINT_LOCK = threading.Lock()

# Un solo formato para la cabecera, el cronometro en vivo y la fila final:
# asi no se pueden desalinear entre si.
LINE = "  {n:>7}  {el:<10} {filas:>5} {tiempo:>8}"


def rule() -> str:
    return LINE.format(n="-" * 7, el="-" * 10, filas="-" * 5, tiempo="-" * 8)


def header() -> str:
    return LINE.format(n="#", el="ELEMENTO", filas="FILAS",
                       tiempo="TIEMPO") + "\n" + rule()


class Ticker:
    """Cronometro en vivo. Reescribe una unica linea con \r (sin scroll),
    con la misma forma que tendra la fila final."""

    def __init__(self, n: str, el: str):
        self.n, self.el = n, el
        self.started = time.monotonic()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._tick, daemon=True)
        self._thread.start()

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self.started

    def _tick(self) -> None:
        while not self._stop.wait(0.1):
            with PRINT_LOCK:
                sys.stderr.write("\r" + LINE.format(
                    n=self.n, el=self.el, filas="",
                    tiempo=f"{self.elapsed:.1f}s"))
                sys.stderr.flush()

    def clear(self) -> None:
        with PRINT_LOCK:
            sys.stderr.write("\r" + " " * 79 + "\r")
            sys.stderr.flush()

    def stop(self) -> float:
        self._stop.set()
        self._thread.join()
        self.clear()
        return self.elapsed


def emit_row(n: int, total: int, element: str, rows, secs: float,
             failed_rc: int | None = None) -> None:
    with PRINT_LOCK:
        sys.stderr.write(LINE.format(
            n=f"{n}/{total}", el=element,
            filas=f"rc{failed_rc}" if failed_rc else len(rows),
            tiempo=f"{secs:.1f}s") + "\n")
        sys.stderr.flush()


def log(msg: str) -> None:
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", file=sys.stderr, flush=True)


def ask_environment() -> tuple[str, str]:
    """Prompt de ambiente. Devuelve (env, stage). Todo va a stderr: en modo
    individual el CSV sale por stdout y el prompt lo corromperia."""
    print("\n  Que ambiente?", file=sys.stderr)
    for i, (label, _, _) in enumerate(ENVIRONMENTS, 1):
        print(f"    {i}) {label}", file=sys.stderr)
    while True:
        print("  Opcion [1]: ", end="", file=sys.stderr, flush=True)
        try:
            raw = input().strip() or "1"
        except (EOFError, KeyboardInterrupt):
            print(file=sys.stderr)
            log("INFO: Sin respuesta; se asume Todos.")
            return "", ""
        if raw.isdigit() and 1 <= int(raw) <= len(ENVIRONMENTS):
            label, environment, stage = ENVIRONMENTS[int(raw) - 1]
            log(f"INFO: Ambiente: {label}"
                + (f" -> --env {environment} --sn {stage}" if environment else ""))
            return environment, stage
        print(f"    Opcion invalida (1-{len(ENVIRONMENTS)}).", file=sys.stderr)


class Config:
    def __init__(self) -> None:
        env = os.environ.get
        self.instance = env("ENDEVOR_INSTANCE", "ENDEVOR")
        self.type = env("ENDEVOR_TYPE", "*")
        # El host responde con la API REST v1 (modo compatibilidad), donde
        # --limit puede no existir. Opt-in: por defecto no se manda.
        self.limit = env("ENDEVOR_LIMIT", "")
        self.environment = ""  # lo define el prompt
        self.stage = ""  # lo define el prompt, atado al ambiente
        self.system = env("ENDEVOR_SYSTEM", "*")
        self.subsystem = env("ENDEVOR_SUBSYSTEM", "*")
        self.delim = env("ENDEVOR_CSV_DELIM", ";")
        self.timeout = int(env("ENDEVOR_TIMEOUT", "300"))
        self.workers = int(env("ENDEVOR_WORKERS", "4"))

# En Windows zowe es un shim .cmd y CreateProcess solo resuelve .exe:
# subprocess no lo encuentra por mas que el PATH este bien. shutil.which
# honra PATHEXT y devuelve la ruta completa al .cmd. En Linux/WSL es un
# no-op que devuelve la misma ruta que resolveria el shell.
ZOWE = shutil.which("zowe") or "zowe"


# En Windows, subprocess.run(timeout=) mata solo el proceso lanzado (el .cmd),
# no el node hijo que abre la conexion al host: ese queda huerfano y colgado.
# Con un grupo de procesos propio podemos matar el arbol entero al vencer.
if os.name == "nt":
    _POPEN_KW = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}

    def _kill_tree(proc):
        # taskkill /T mata el proceso y toda su descendencia.
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                       capture_output=True)
else:
    _POPEN_KW = {"start_new_session": True}

    def _kill_tree(proc):
        import signal
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            time.sleep(0.5)
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass


def run_zowe(args: list[str], timeout: int) -> tuple[int, str]:
    try:
        proc = subprocess.Popen([ZOWE, *args], stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True,
                                encoding="utf-8", errors="replace", **_POPEN_KW)
    except OSError as exc:
        return 126, str(exc)
    try:
        out, err = proc.communicate(timeout=timeout)
        return proc.returncode, (out or "") + (err or "")
    except subprocess.TimeoutExpired:
        _kill_tree(proc)
        try:
            proc.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            pass
        return 124, f"timeout tras {timeout}s (proceso terminado)"


def run_zowe_retry(args: list[str], timeout: int, retries: int = 1) -> tuple[int, str]:
    """Reintenta una vez ante timeout o fallo transitorio de arranque."""
    rc, out = run_zowe(args, timeout)
    attempt = 0
    while rc in (124, 126) and attempt < retries:
        attempt += 1
        rc, out = run_zowe(args, timeout)
    return rc, out


def build_args(cfg: Config, element: str) -> list[str]:
    """El minimo probado. Un filtro en '*' no se manda: omitirlo ya significa
    'todos', y --rff sobra porque filtra la tabla, no el 'data' del JSON."""
    args = ["endevor", "list", "elements", element, "-i", cfg.instance, "--rfj"]
    for flag, value in (("--typ", cfg.type),
                        ("--env", cfg.environment),
                        ("--sn", cfg.stage),
                        ("--sys", cfg.system),
                        ("--sub", cfg.subsystem)):
        if value and value != "*":
            args += [flag, value]
    if cfg.limit:
        args += ["--limit", cfg.limit]
    return args


def _walk(node) -> "list[dict]":
    """Encuentra objetos con elmName a cualquier profundidad del JSON."""
    found = []
    if isinstance(node, dict):
        if "elmName" in node:
            found.append(node)
        for value in node.values():
            found.extend(_walk(value))
    elif isinstance(node, list):
        for value in node:
            found.extend(_walk(value))
    return found


def fmt_date(value: str) -> str:
    """2018-05-17T15:54:00.00+0000 -> 2018-05-17 15:54:00"""
    m = ISO_RE.match(value)
    return f"{m.group(1)} {m.group(2)}" if m else value


def parse_json(text: str) -> list[list[str]]:
    try:
        doc = json.loads(text)
    except json.JSONDecodeError:
        log("WARN: La respuesta de --rfj no es JSON valido.")
        return []
    rows = []
    for obj in _walk(doc):
        row = []
        for key in KEYS:
            value = str(obj.get(key) or "").strip()
            row.append(fmt_date(value) if key in DATE_KEYS else value)
        rows.append(row)
    return rows


def read_input(path: str) -> list[str]:
    """Solo el nombre del elemento. Si hay mas columnas, se ignoran."""
    elements: "OrderedDict[str, None]" = OrderedDict()
    with open(path, encoding="utf-8", errors="replace") as fh:
        for raw in fh:
            line = re.sub(r"[;,\t]", " ", raw.strip())
            if not line or line.startswith("#"):
                continue
            name = line.split()[0].upper()
            if name == "ELEMENT":
                continue
            if not FIELD_RE.match(name):
                log(f"WARN: Nombre invalido, se omite: {name!r}")
                continue
            elements.setdefault(name)
    return list(elements)


def execute(cfg: Config, elements: list[str], sink: CsvSink) -> int:
    """PARALELO: cfg.workers a la vez. Cada resultado se escribe al CSV en
    cuanto llega (orden de termino); el sink hace flush por fila, asi que un
    Ctrl+C o timeout conserva lo ya consultado. main() reordena al cerrar.
    Nota: los threads ya en vuelo al momento del Ctrl+C terminan su zowe
    (o su timeout) antes de soltar; lo escrito antes esta a salvo igual."""
    failed = 0
    total = len(elements)
    print(header(), file=sys.stderr, flush=True)

    def task(element: str):
        t = time.monotonic()
        rc, out = run_zowe_retry(build_args(cfg, element), cfg.timeout)
        return element, rc, out, time.monotonic() - t

    ticker = Ticker("...", f"{cfg.workers} en vuelo")
    done = 0
    pool = ThreadPoolExecutor(max_workers=cfg.workers)
    futures = [pool.submit(task, e) for e in elements]
    try:
        for future in as_completed(futures):
            element, rc, out, secs = future.result()
            done += 1
            ticker.clear()
            if rc != 0:
                emit_row(done, total, element, [], secs, failed_rc=rc)
                failed += 1
                continue
            found = parse_json(out)
            sink.add(found)
            emit_row(done, total, element, found, secs)
    except KeyboardInterrupt:
        ticker.stop()
        pool.shutdown(wait=False, cancel_futures=True)
        raise
    ticker.stop()
    pool.shutdown(wait=True)
    return failed


def reconcile(elements: list[str], rows: list[list[str]]) -> list[list[str]]:
    """Cruza lo pedido contra lo obtenido, en el orden del input. La clave es
    solo el nombre: un elemento puede volver bajo varios types -> varias filas."""
    index: "defaultdict[str, list[list[str]]]" = defaultdict(list)
    seen: set[tuple[str, ...]] = set()
    for row in rows:
        key = tuple(row)
        if key in seen:
            continue
        seen.add(key)
        index[row[0].upper()].append(row)

    out = []
    for name in elements:
        matches = index.get(name)
        if matches:
            out.extend([*row, "FOUND"] for row in matches)
        else:
            out.append([name, *[""] * (len(KEYS) - 1), "NOT_FOUND"])
    return out


class CsvSink:
    """Escribe el CSV a medida que llegan las filas y hace flush por cada una,
    para que un Ctrl+C, un timeout o un cuelgue no pierdan lo ya recolectado.
    Registra que elementos produjeron datos; los que no, se emiten como
    NOT_FOUND al cerrar."""

    def __init__(self, path: str, delim: str):
        self.path = path
        self.fh = open(path, "w", newline="", encoding="utf-8")
        self.writer = csv.writer(self.fh, delimiter=delim, lineterminator="\r\n",
                                 quoting=csv.QUOTE_MINIMAL)
        self.writer.writerow(COLS)
        self.fh.flush()
        self.seen: set[str] = set()
        self.rows_written = 0

    def add(self, rows: list[list[str]]) -> None:
        for row in rows:
            self.writer.writerow([*row, "FOUND"])
            self.seen.add(row[0].upper())
            self.rows_written += 1
        if rows:
            self.fh.flush()

    def finish_missing(self, elements: list[str]) -> int:
        """Cierra el CSV agregando NOT_FOUND para lo que nunca aparecio."""
        missing = 0
        for name in elements:
            if name not in self.seen:
                self.writer.writerow([name, *[""] * (len(KEYS) - 1), "NOT_FOUND"])
                missing += 1
        self.fh.flush()
        return missing

    def close(self) -> None:
        if not self.fh.closed:
            self.fh.close()


def reorder_csv(path: str, elements: list[str], delim: str) -> None:
    """Reordena el CSV de orden-de-termino a orden-de-input. Best-effort:
    ante cualquier fallo, el CSV desordenado ya es valido y se deja igual."""
    try:
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh, delimiter=delim)
            header_row = next(reader)
            rows = list(reader)
    except (OSError, StopIteration):
        return
    order = {name: i for i, name in enumerate(elements)}
    rows.sort(key=lambda r: order.get(r[0].upper(), len(order)))
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, delimiter=delim, lineterminator="\r\n",
                            quoting=csv.QUOTE_MINIMAL)
        writer.writerow(header_row)
        writer.writerows(rows)


def main(argv: list[str]) -> int:
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__)
        return 1 if not argv else 0

    cfg = Config()
    arg1 = argv[0]
    arg2 = argv[1] if len(argv) > 1 else None

    started = datetime.now()
    t0 = time.monotonic()

    stamp = started.strftime("%Y%m%d_%H%M%S")
    if isfile(arg1):
        elements = read_input(arg1)
        base = splitext(basename(arg1))[0]
    else:
        elements = [arg1.upper()]
        base = elements[0]
    output = arg2 or f"{base}_{stamp}.csv"

    if not elements:
        log("ERROR: No hay elementos validos en el archivo de entrada.")
        return 2

    cfg.environment, cfg.stage = ask_environment()

    log(f"INFO: {len(elements)} elementos | modo paralelo ({cfg.workers} workers)")

    sink = CsvSink(output, cfg.delim)
    interrupted = False
    failed = 0
    try:
        failed = execute(cfg, elements, sink)
    except KeyboardInterrupt:
        interrupted = True
        print(file=sys.stderr)
        log("INTERRUMPIDO: guardando lo recolectado hasta ahora...")
    finally:
        missing = sink.finish_missing(elements)
        found = sink.rows_written
        sink.close()
        # en paralelo el CSV quedo en orden de termino; reordenar a orden de
        # input solo si termino entero (si se corto, dejar lo que hay).
        if not interrupted:
            reorder_csv(output, elements, cfg.delim)

    elapsed = time.monotonic() - t0
    print(rule(), file=sys.stderr)
    parts = [f"{len(elements)} elementos", f"{found} filas"]
    if missing:
        parts.append(f"{missing} sin datos")
    if failed:
        parts.append(f"{failed} FALLIDAS")
    print(f"  {' | '.join(parts)}", file=sys.stderr)
    print(f"  Tiempo total: {elapsed:.1f}s  "
          f"({elapsed / len(elements):.1f}s por elemento)\n", file=sys.stderr)

    log(f"Archivo generado: {abspath(output)}")
    if interrupted:
        log("NOTA: corrida incompleta; el CSV tiene solo lo consultado antes del corte.")
        return 130
    return 4 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
