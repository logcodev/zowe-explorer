#!/usr/bin/env python3
"""
RecuperaEndevor_seq.py - Inventario de elementos Endevor via Zowe CLI -> CSV
                         VERSION SECUENCIAL: una consulta a la vez.

Autor   : Albert Tapia
Version : 6.0

El input es una lista de NOMBRES DE ELEMENTO. El TYPE es un dato a
descubrir, no un filtro: se consulta con --typ * y se reporta lo que venga.
Un mismo elemento puede existir bajo varios types -> varias filas.
Los elementos que no existen salen con STATUS=NOT_FOUND.

Uso:
    RecuperaEndevor_seq.py <archivo_input> [salida.csv]
    RecuperaEndevor_seq.py <ELEMENT>

Variables de entorno opcionales:
    ENDEVOR_TYPE            (default: *)   filtro de type, si lo conoces
    ENDEVOR_LIMIT           (default: 0)   0 = NOLIMIT (filtramos en local)
    ENDEVOR_INSTANCE        (default: ENDEVOR)
    ENDEVOR_ENVIRONMENT     (default: *)
    ENDEVOR_STAGE           (default: *)
    ENDEVOR_SYSTEM          (default: *)
    ENDEVOR_SUBSYSTEM       (default: *)
    ENDEVOR_CSV_DELIM       (default: ;)
    ENDEVOR_TIMEOUT         (default: 300) segundos por consulta

Codigos de salida:
    0 ok | 1 uso | 2 input | 4 fallos parciales | 8 fallaron todas
"""

from __future__ import annotations

import csv
import json
import os
import re
import subprocess
import sys
from collections import OrderedDict, defaultdict
from datetime import datetime
from os.path import basename, isfile, splitext

FIELDS = ["elmName", "typeName", "envName", "stgId", "sysName",
          "sbsName", "procGrpName", "signoutId", "lastActCcid", "lastActDate"]

COLS = ["ELEMENT", "TYPE", "ENV", "STAGE", "SYSTEM", "SUBSYSTEM",
        "PROCGROUP", "SIGNOUT", "CCID", "DATETIME", "STATUS"]

# Charset de FIELD_RE de la extension VSCode, menos los comodines (* %):
# aqui cada nombre se cruza 1:1 contra la respuesta, y una mascara no
# tendria contra que cruzarse.
FIELD_RE = re.compile(r"^[A-Za-z0-9$#@._-]+$")


def log(msg: str) -> None:
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", file=sys.stderr, flush=True)


class Config:
    def __init__(self) -> None:
        env = os.environ.get
        self.instance = env("ENDEVOR_INSTANCE", "ENDEVOR")
        self.type = env("ENDEVOR_TYPE", "*")
        # El help documenta 0=NOLIMIT pero NO el default. Explicito siempre:
        # filtramos en local, asi que cualquier tope solo trunca en silencio.
        self.limit = env("ENDEVOR_LIMIT", "0")
        self.environment = env("ENDEVOR_ENVIRONMENT", "*")
        self.stage = env("ENDEVOR_STAGE", "*")
        self.system = env("ENDEVOR_SYSTEM", "*")
        self.subsystem = env("ENDEVOR_SUBSYSTEM", "*")
        self.delim = env("ENDEVOR_CSV_DELIM", ";")
        self.timeout = int(env("ENDEVOR_TIMEOUT", "300"))

    @property
    def wildcards(self) -> bool:
        return "*" in (self.type, self.environment, self.stage,
                       self.system, self.subsystem)


def run_zowe(args: list[str], timeout: int) -> tuple[int, str]:
    try:
        p = subprocess.run(["zowe", *args], capture_output=True, text=True,
                           timeout=timeout, encoding="utf-8", errors="replace")
        return p.returncode, (p.stdout or "") + (p.stderr or "")
    except subprocess.TimeoutExpired:
        return 124, f"timeout tras {timeout}s"
    except OSError as exc:
        return 126, str(exc)


def build_args(cfg: Config, element: str) -> list[str]:
    args = ["endevor", "list", "elements", element,
            "--typ", cfg.type,
            "-i", cfg.instance,
            "--env", cfg.environment,
            "--sn", cfg.stage,
            "--sys", cfg.system,
            "--sub", cfg.subsystem,
            "--ret", "all",
            "--dat", "all",
            "--sm",
            "--rff", *FIELDS,
            "--limit", cfg.limit,
            "--rfj"]
    # --sea (search) no admite comodines en los niveles del mapa.
    # --search-limit solo aplica cuando se busca por el mapa.
    if not cfg.wildcards:
        args += ["--sea", "--search-limit", cfg.limit]
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


def parse_json(text: str) -> list[list[str]]:
    try:
        doc = json.loads(text)
    except json.JSONDecodeError:
        log("WARN: La respuesta de --rfj no es JSON valido.")
        return []
    return [[str(o.get(f) or "").strip() for f in FIELDS] for o in _walk(doc)]


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


def execute(cfg: Config, elements: list[str]) -> tuple[list[list[str]], int]:
    """SECUENCIAL: un elemento a la vez."""
    rows: list[list[str]] = []
    failed = 0
    total = len(elements)
    for n, element in enumerate(elements, 1):
        rc, out = run_zowe(build_args(cfg, element), cfg.timeout)
        if rc != 0:
            log(f"WARN: [{n}/{total}] Fallo consulta elemento='{element}' rc={rc}")
            failed += 1
            continue
        found = parse_json(out)
        rows.extend(found)
        log(f"INFO: [{n}/{total}] {element}: {len(found)} filas")
    return rows, failed


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
            out.append([name, *[""] * 9, "NOT_FOUND"])
    return out


def write_csv(rows: list[list[str]], delim: str, path: str | None) -> None:
    fh = open(path, "w", newline="", encoding="utf-8") if path else sys.stdout
    try:
        writer = csv.writer(fh, delimiter=delim, lineterminator="\r\n",
                            quoting=csv.QUOTE_MINIMAL)
        writer.writerow(COLS)
        writer.writerows(rows)
    finally:
        if path:
            fh.close()


def main(argv: list[str]) -> int:
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__)
        return 1 if not argv else 0

    cfg = Config()
    arg1 = argv[0]
    arg2 = argv[1] if len(argv) > 1 else None

    started = datetime.now()
    log(f"====== INICIO PROCESO : {started:%Y-%m-%d %H:%M:%S} ======")

    if isfile(arg1):
        elements = read_input(arg1)
        stamp = started.strftime("%Y%m%d_%H%M%S")
        output = arg2 or f"{splitext(basename(arg1))[0]}_{stamp}.csv"
    else:
        elements = [arg1.upper()]
        output = None  # stdout

    if not elements:
        log("ERROR: No hay elementos validos en el archivo de entrada.")
        return 2

    log(f"INFO: {len(elements)} elementos | modo secuencial")

    rows, failed = execute(cfg, elements)

    if failed and failed == len(elements):
        log(f"ERROR: Fallaron todas las consultas ({failed}/{len(elements)}).")
        log("Sugerencia: valida perfiles Zowe/Endevor y ENDEVOR_INSTANCE.")
        return 8

    if not rows and not failed:
        log("WARN: Cero filas en todas las consultas. Si esperabas datos,")
        log("      revisa que el JSON de --rfj traiga objetos con elmName.")

    final = reconcile(elements, rows)
    write_csv(final, cfg.delim, output)

    found = sum(1 for r in final if r[-1] == "FOUND")
    missing = sum(1 for r in final if r[-1] == "NOT_FOUND")
    if output:
        log(f"INFO: Archivo generado: {output}")
    log(f"INFO: Filas con datos: {found} | Elementos no encontrados: {missing}")
    if failed:
        log(f"WARN: Consultas fallidas: {failed}/{len(elements)}")

    ended = datetime.now()
    elapsed = (ended - started).total_seconds()
    log(f"====== FIN PROCESO    : {ended:%Y-%m-%d %H:%M:%S} "
        f"({elapsed:.1f}s | {elapsed / len(elements):.1f}s por elemento) ======")
    return 4 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
