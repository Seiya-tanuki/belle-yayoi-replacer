# -*- coding: utf-8 -*-
from __future__ import annotations

"""
Yayoi import CSV canonical column indices (0-based, total 25 columns).

Note:
- Receipt line (current runtime) uses only:
  - COL_DEBIT_ACCOUNT
  - COL_SUMMARY
"""

COL_DATE = 3
COL_DEBIT_ACCOUNT = 4
COL_DEBIT_SUBACCOUNT = 5
COL_DEBIT_TAX_DIVISION = 7
COL_DEBIT_AMOUNT = 8
COL_DEBIT_TAX_AMOUNT = 9
COL_CREDIT_ACCOUNT = 10
COL_CREDIT_SUBACCOUNT = 11
COL_CREDIT_TAX_DIVISION = 13
COL_CREDIT_AMOUNT = 14
COL_CREDIT_TAX_AMOUNT = 15
COL_SUMMARY = 16
COL_MEMO = 21
