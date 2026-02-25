from __future__ import annotations

from .yayoi_csv import text_to_token, token_to_text


def safe_cell_text(tokens: list[bytes], idx: int, encoding: str) -> str:
    if idx < 0 or idx >= len(tokens):
        return ""
    tok = tokens[idx]
    if isinstance(tok, bytes):
        return token_to_text(tok, encoding)
    return str(tok)


def set_cell_text(tokens: list[bytes], idx: int, encoding: str, value: str) -> None:
    tokens[idx] = text_to_token(value, encoding, template_token=tokens[idx])
