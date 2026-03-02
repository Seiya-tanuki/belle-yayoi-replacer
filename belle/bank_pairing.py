# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, TypeVar
import re
import unicodedata

from .yayoi_text import safe_cell_text
from .yayoi_columns import (
    COL_CREDIT_ACCOUNT,
    COL_CREDIT_AMOUNT,
    COL_CREDIT_SUBACCOUNT,
    COL_CREDIT_TAX_DIVISION,
    COL_DATE,
    COL_DEBIT_ACCOUNT,
    COL_DEBIT_AMOUNT,
    COL_DEBIT_SUBACCOUNT,
    COL_DEBIT_TAX_DIVISION,
    COL_MEMO,
    COL_SUMMARY,
)
from .yayoi_csv import read_yayoi_csv

JoinKey = Tuple[str, str, int]
_SEPARATOR_RE = re.compile(
    r"[ \t\r\n\u3000/／\\|｜･・,，、。:：;；_\-‐‑‒–—―ーｰ~〜\(\)（）\[\]［］\{\}｛｝\"'`「」『』]+"
)
_SIGN_RE = re.compile(r"SIGN\s*=\s*(debit|credit)", flags=re.IGNORECASE)
_YMD_SLASH_RE = re.compile(r"^(\d{4})[/-](\d{1,2})[/-](\d{1,2})$")
_YMD_JP_RE = re.compile(r"^(\d{4})年(\d{1,2})月(\d{1,2})日$")
_YMD_COMPACT_RE = re.compile(r"^(\d{8})$")
_WAREKI_RE = re.compile(r"^([Rr])\.?\s*(\d{1,2})[/-](\d{1,2})[/-](\d{1,2})$")


def _safe_text(tokens: Sequence[bytes], idx: int, encoding: str) -> str:
    return safe_cell_text(tokens, idx, encoding)


def _normalize_name_for_match(text: str) -> str:
    s = unicodedata.normalize("NFKC", text or "")
    return re.sub(r"[ \u3000]+", "", s).strip()


def _normalize_date_key(text: str) -> Optional[str]:
    s = unicodedata.normalize("NFKC", text or "").strip()
    if not s:
        return None

    m = _YMD_SLASH_RE.match(s)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return date(y, mo, d).isoformat()
        except ValueError:
            return None

    m = _YMD_JP_RE.match(s)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return date(y, mo, d).isoformat()
        except ValueError:
            return None

    m = _YMD_COMPACT_RE.match(s)
    if m:
        raw = m.group(1)
        y, mo, d = int(raw[0:4]), int(raw[4:6]), int(raw[6:8])
        try:
            return date(y, mo, d).isoformat()
        except ValueError:
            return None

    m = _WAREKI_RE.match(s)
    if m:
        era = m.group(1).upper()
        era_year = int(m.group(2))
        mo, d = int(m.group(3)), int(m.group(4))
        if era != "R" or era_year < 1:
            return None
        gregorian_year = 2018 + era_year
        try:
            return date(gregorian_year, mo, d).isoformat()
        except ValueError:
            return None

    try:
        return date.fromisoformat(s.replace("/", "-")).isoformat()
    except ValueError:
        return None


def normalize_kana_key(text: str) -> str:
    s = unicodedata.normalize("NFKC", text or "").strip()
    s = _SEPARATOR_RE.sub("", s)
    return s.upper()


def _parse_amount_cell(text: str) -> Optional[int]:
    s = unicodedata.normalize("NFKC", text or "").strip()
    if not s:
        return 0
    s = s.replace(",", "").replace("，", "").replace("¥", "").replace("￥", "")
    s = re.sub(r"[ \u3000]", "", s)
    if s.startswith("(") and s.endswith(")") and len(s) > 2:
        s = "-" + s[1:-1]
    if not re.fullmatch(r"[+-]?\d+", s):
        return None
    try:
        return abs(int(s))
    except Exception:
        return None


def parse_amount(tokens: Sequence[bytes], encoding: str) -> Optional[int]:
    debit_raw = _safe_text(tokens, COL_DEBIT_AMOUNT, encoding)
    credit_raw = _safe_text(tokens, COL_CREDIT_AMOUNT, encoding)

    debit_amt = _parse_amount_cell(debit_raw)
    credit_amt = _parse_amount_cell(credit_raw)
    if debit_amt is None or credit_amt is None:
        return None

    if debit_amt > 0 and credit_amt > 0 and debit_amt != credit_amt:
        return None
    if debit_amt <= 0 and credit_amt <= 0:
        return None
    if debit_amt > 0:
        return int(debit_amt)
    return int(credit_amt)


def derive_sign_from_accounts(
    tokens: Sequence[bytes],
    encoding: str,
    bank_account_name: str,
    bank_account_subaccount: str,
) -> Optional[str]:
    bank_name_key = _normalize_name_for_match(bank_account_name)
    bank_sub_key = _normalize_name_for_match(bank_account_subaccount)
    if not bank_name_key:
        return None

    debit_account = _safe_text(tokens, COL_DEBIT_ACCOUNT, encoding)
    credit_account = _safe_text(tokens, COL_CREDIT_ACCOUNT, encoding)
    debit_sub = _safe_text(tokens, COL_DEBIT_SUBACCOUNT, encoding)
    credit_sub = _safe_text(tokens, COL_CREDIT_SUBACCOUNT, encoding)

    def _is_bank_side(account_text: str, subaccount_text: str) -> bool:
        if _normalize_name_for_match(account_text) != bank_name_key:
            return False
        if bank_sub_key:
            return _normalize_name_for_match(subaccount_text) == bank_sub_key
        return True

    bank_on_debit = _is_bank_side(debit_account, debit_sub)
    bank_on_credit = _is_bank_side(credit_account, credit_sub)

    if bank_on_debit and bank_on_credit:
        return None
    if bank_on_debit:
        return "credit"
    if bank_on_credit:
        return "debit"
    return None


def extract_teacher_bank_subaccount(
    tokens: Sequence[bytes],
    encoding: str,
    bank_account_name: str,
) -> str:
    bank_name_key = _normalize_name_for_match(bank_account_name)
    if not bank_name_key:
        return ""

    debit_account = _safe_text(tokens, COL_DEBIT_ACCOUNT, encoding)
    credit_account = _safe_text(tokens, COL_CREDIT_ACCOUNT, encoding)
    if _normalize_name_for_match(debit_account) == bank_name_key:
        return _safe_text(tokens, COL_DEBIT_SUBACCOUNT, encoding)
    if _normalize_name_for_match(credit_account) == bank_name_key:
        return _safe_text(tokens, COL_CREDIT_SUBACCOUNT, encoding)
    return ""


def extract_sign_from_memo(tokens: Sequence[bytes], encoding: str) -> Optional[str]:
    memo = unicodedata.normalize("NFKC", _safe_text(tokens, COL_MEMO, encoding))
    if not memo:
        return None
    hits = [h.lower() for h in _SIGN_RE.findall(memo)]
    if not hits:
        return None
    unique = sorted(set(hits))
    if len(unique) != 1:
        return None
    return unique[0]


def _derive_effective_sign_with_mismatch(
    tokens: Sequence[bytes],
    encoding: str,
    bank_account_name: str,
    bank_account_subaccount: str,
) -> Tuple[Optional[str], bool]:
    derived = derive_sign_from_accounts(
        tokens,
        encoding,
        bank_account_name=bank_account_name,
        bank_account_subaccount=bank_account_subaccount,
    )
    memo_sign = extract_sign_from_memo(tokens, encoding)
    if derived:
        if memo_sign and memo_sign != derived:
            return None, True
        return derived, False
    if memo_sign:
        return memo_sign, False
    return None, False


def derive_effective_sign(
    tokens: Sequence[bytes],
    encoding: str,
    bank_account_name: str,
    bank_account_subaccount: str,
) -> Optional[str]:
    sign, _mismatch = _derive_effective_sign_with_mismatch(
        tokens,
        encoding,
        bank_account_name=bank_account_name,
        bank_account_subaccount=bank_account_subaccount,
    )
    return sign


T = TypeVar("T")


def build_unique_index(records: Sequence[Tuple[JoinKey, T]]) -> Tuple[Dict[JoinKey, T], Set[JoinKey]]:
    index: Dict[JoinKey, T] = {}
    dup_keys: Set[JoinKey] = set()
    for join_key, value in records:
        if join_key in dup_keys:
            continue
        if join_key in index:
            dup_keys.add(join_key)
            index.pop(join_key, None)
            continue
        index[join_key] = value
    return index, dup_keys


def build_training_pairs(
    ocr_csv_path: Path,
    ref_csv_path: Path,
    config: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    bank_account_name = str(config.get("bank_account_name") or "普通預金")
    bank_account_subaccount = str(config.get("bank_account_subaccount") or "")

    ocr_csv = read_yayoi_csv(ocr_csv_path)
    ref_csv = read_yayoi_csv(ref_csv_path)

    metrics: Dict[str, int] = {
        "rows_total_ocr": int(len(ocr_csv.rows)),
        "rows_valid_ocr": 0,
        "ocr_dup_keys": 0,
        "rows_total_reference": int(len(ref_csv.rows)),
        "ref_rows_valid": 0,
        "ref_dup_keys": 0,
        "pairs_unique_used": 0,
        "pairs_missing_skipped": 0,
        "sign_mismatch_skipped": 0,
    }

    ocr_records: List[Tuple[JoinKey, Dict[str, Any]]] = []
    for row_index, row in enumerate(ocr_csv.rows, start=1):
        tokens = row.tokens
        date_key = _normalize_date_key(_safe_text(tokens, COL_DATE, ocr_csv.encoding))
        if not date_key:
            continue
        amount = parse_amount(tokens, ocr_csv.encoding)
        if amount is None:
            continue
        sign, mismatch = _derive_effective_sign_with_mismatch(
            tokens,
            ocr_csv.encoding,
            bank_account_name=bank_account_name,
            bank_account_subaccount=bank_account_subaccount,
        )
        if mismatch:
            metrics["sign_mismatch_skipped"] += 1
            continue
        if not sign:
            continue
        summary = _safe_text(tokens, COL_SUMMARY, ocr_csv.encoding)
        kana_key = normalize_kana_key(summary)
        if not kana_key:
            continue

        join_key: JoinKey = (date_key, sign, int(amount))
        ocr_records.append(
            (
                join_key,
                {
                    "row_index_1b": int(row_index),
                    "kana_key": kana_key,
                    "sign": sign,
                    "amount": int(amount),
                    "summary": summary,
                },
            )
        )
        metrics["rows_valid_ocr"] += 1

    ref_records: List[Tuple[JoinKey, Dict[str, Any]]] = []
    for row_index, row in enumerate(ref_csv.rows, start=1):
        tokens = row.tokens
        date_key = _normalize_date_key(_safe_text(tokens, COL_DATE, ref_csv.encoding))
        if not date_key:
            continue
        amount = parse_amount(tokens, ref_csv.encoding)
        if amount is None:
            continue

        sign_from_accounts = derive_sign_from_accounts(
            tokens,
            ref_csv.encoding,
            bank_account_name=bank_account_name,
            bank_account_subaccount=bank_account_subaccount,
        )
        memo_sign = extract_sign_from_memo(tokens, ref_csv.encoding)
        if sign_from_accounts and memo_sign and memo_sign != sign_from_accounts:
            metrics["sign_mismatch_skipped"] += 1
            continue
        sign = sign_from_accounts or memo_sign
        if not sign:
            continue
        # Teacher-side counter fields must be deterministic from bank side.
        if sign_from_accounts is None:
            continue

        if sign == "debit":
            counter_account = _safe_text(tokens, COL_DEBIT_ACCOUNT, ref_csv.encoding)
            counter_subaccount = _safe_text(tokens, COL_DEBIT_SUBACCOUNT, ref_csv.encoding)
            counter_tax_division = _safe_text(tokens, COL_DEBIT_TAX_DIVISION, ref_csv.encoding)
        else:
            counter_account = _safe_text(tokens, COL_CREDIT_ACCOUNT, ref_csv.encoding)
            counter_subaccount = _safe_text(tokens, COL_CREDIT_SUBACCOUNT, ref_csv.encoding)
            counter_tax_division = _safe_text(tokens, COL_CREDIT_TAX_DIVISION, ref_csv.encoding)
        bank_subaccount = extract_teacher_bank_subaccount(
            tokens,
            ref_csv.encoding,
            bank_account_name=bank_account_name,
        )

        corrected_summary = _safe_text(tokens, COL_SUMMARY, ref_csv.encoding)
        if not corrected_summary or not counter_account:
            continue

        join_key = (date_key, sign, int(amount))
        ref_records.append(
            (
                join_key,
                {
                    "row_index_1b": int(row_index),
                    "corrected_summary": corrected_summary,
                    "counter_account": counter_account,
                    "counter_subaccount": counter_subaccount,
                    "counter_tax_division": counter_tax_division,
                    "bank_subaccount": bank_subaccount,
                    "sign": sign,
                    "amount": int(amount),
                },
            )
        )
        metrics["ref_rows_valid"] += 1

    ocr_index, ocr_dup_keys = build_unique_index(ocr_records)
    ref_index, ref_dup_keys = build_unique_index(ref_records)
    metrics["ocr_dup_keys"] = int(len(ocr_dup_keys))
    metrics["ref_dup_keys"] = int(len(ref_dup_keys))

    unique_ocr_keys = set(ocr_index.keys())
    unique_ref_keys = set(ref_index.keys())
    common_keys = sorted(unique_ocr_keys.intersection(unique_ref_keys))

    pairs: List[Dict[str, Any]] = []
    for key in common_keys:
        date_key, sign, amount = key
        pairs.append(
            {
                "join_key": key,
                "date": date_key,
                "sign": sign,
                "amount": int(amount),
                "ocr": ocr_index[key],
                "teacher": ref_index[key],
            }
        )

    metrics["pairs_unique_used"] = int(len(pairs))
    metrics["pairs_missing_skipped"] = int(len(unique_ocr_keys ^ unique_ref_keys))

    return pairs, metrics
