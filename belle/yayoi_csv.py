# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

EXPECTED_COLS = 25
YAYOI_ENCODING = "cp932"
YAYOI_LINE_ENDING = b"\r\n"


@dataclass
class YayoiRow:
    tokens: List[bytes]  # length == 25
    eol: bytes


@dataclass
class YayoiCSV:
    path: Path
    encoding: str
    line_ending: bytes
    rows: List[YayoiRow]


def _is_blank_row(row: List[str]) -> bool:
    return len(row) == 0


def read_yayoi_csv(path: Path, expected_cols: int = EXPECTED_COLS) -> YayoiCSV:
    rows: List[YayoiRow] = []
    with path.open("r", encoding=YAYOI_ENCODING, newline="") as f:
        reader = csv.reader(f, dialect="excel")
        for row_idx_1b, row in enumerate(reader, start=1):
            if _is_blank_row(row):
                continue
            if len(row) != expected_cols:
                raise ValueError(
                    f"CSV column count mismatch at {path} row={row_idx_1b}: "
                    f"expected {expected_cols}, got {len(row)}"
                )
            tokens = [c.encode(YAYOI_ENCODING, errors="strict") for c in row]
            rows.append(YayoiRow(tokens=tokens, eol=YAYOI_LINE_ENDING))
    return YayoiCSV(path=path, encoding=YAYOI_ENCODING, line_ending=YAYOI_LINE_ENDING, rows=rows)


def token_to_text(tok: bytes, encoding: str) -> str:
    return tok.decode(encoding, errors="replace")


def text_to_token(text: str, encoding: str, template_token: Optional[bytes] = None) -> bytes:
    _ = template_token  # kept for signature compatibility
    return text.encode(encoding, errors="strict")


def write_yayoi_csv(csv_obj: YayoiCSV, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding=YAYOI_ENCODING, newline="") as f:
        writer = csv.writer(f, dialect="excel", lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL)
        for row_idx_1b, row in enumerate(csv_obj.rows, start=1):
            if len(row.tokens) != EXPECTED_COLS:
                raise ValueError(
                    f"CSV column count mismatch at {csv_obj.path} row={row_idx_1b}: "
                    f"expected {EXPECTED_COLS}, got {len(row.tokens)}"
                )
            writer.writerow([token_to_text(tok, csv_obj.encoding) for tok in row.tokens])
