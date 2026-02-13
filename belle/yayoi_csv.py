# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple, Optional

from .encoding import detect_encoding, detect_line_ending


@dataclass
class YayoiRow:
    tokens: List[bytes]   # length == 25
    eol: bytes            # line ending bytes (usually b"\r\n")


@dataclass
class YayoiCSV:
    path: Path
    encoding: str
    line_ending: bytes
    rows: List[YayoiRow]


def _split_line_keep_eol(raw_line: bytes) -> Tuple[bytes, bytes]:
    if raw_line.endswith(b"\r\n"):
        return raw_line[:-2], b"\r\n"
    if raw_line.endswith(b"\n"):
        return raw_line[:-1], b"\n"
    if raw_line.endswith(b"\r"):
        return raw_line[:-1], b"\r"
    return raw_line, b""


def read_yayoi_csv(path: Path, expected_cols: int = 25) -> YayoiCSV:
    data = path.read_bytes()
    enc = detect_encoding(data)
    le = detect_line_ending(data)

    rows: List[YayoiRow] = []
    for raw_line in data.splitlines(keepends=True):
        body, eol = _split_line_keep_eol(raw_line)
        if not body and not eol:
            continue
        # Skip empty trailing lines
        if body.strip() == b"":
            continue
        tokens = body.split(b",")
        if len(tokens) != expected_cols:
            raise ValueError(f"CSV column count mismatch: expected {expected_cols}, got {len(tokens)} at {path}")
        rows.append(YayoiRow(tokens=tokens, eol=eol or le))
    return YayoiCSV(path=path, encoding=enc, line_ending=le, rows=rows)


def _unquote_token(tok: bytes) -> bytes:
    if len(tok) >= 2 and tok[:1] == b'"' and tok[-1:] == b'"':
        inner = tok[1:-1]
        return inner.replace(b'""', b'"')
    return tok


def token_to_text(tok: bytes, encoding: str) -> str:
    b = _unquote_token(tok)
    return b.decode(encoding, errors="replace")


def _quote_bytes(b: bytes) -> bytes:
    return b'"' + b.replace(b'"', b'""') + b'"'


def text_to_token(text: str, encoding: str, template_token: Optional[bytes] = None) -> bytes:
    b = text.encode(encoding, errors="strict")
    # Preserve quoting style if template is quoted
    if template_token is not None and len(template_token) >= 2 and template_token[:1] == b'"' and template_token[-1:] == b'"':
        return _quote_bytes(b)
    # Quote only if needed
    if b"," in b or b'"' in b or b"\n" in b or b"\r" in b:
        return _quote_bytes(b)
    return b


def write_yayoi_csv(csv: YayoiCSV, out_path: Path) -> None:
    out_parts: List[bytes] = []
    for row in csv.rows:
        out_parts.append(b",".join(row.tokens) + (row.eol or csv.line_ending))
    out_path.write_bytes(b"".join(out_parts))
