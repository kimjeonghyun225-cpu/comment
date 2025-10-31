# -*- coding: utf-8 -*-
"""
QA 통합 패치 모듈 (B-1, B-3, B-4)
- B-1: 테스트 시트 자동 감지 + 멀티 선택 유틸
- B-3: '셀 코멘트 + 비고/Notes/Comment/코멘트' 병합
- B-4: LLM JSON 강제 + Excel 리포트 생성(XlsxWriter)
"""
from __future__ import annotations

import re
import json
from typing import List, Dict, Any, Iterable, Optional

import pandas as pd

# ==============================
# B-1. 테스트 시트 자동 감지
# ==============================
TEST_SHEET_PATTERNS = [
    r"(?i)^test[-_ ]?case[_- ]?aos$",
    r"(?i)^test[-_ ]?case[_- ]?ios$",
    r"(?i)^compatibility.*aos",
    r"(?i)^compatibility.*ios",
]

def find_test_sheet_candidates(xls: pd.ExcelFile, patterns: Iterable[str] = TEST_SHEET_PATTERNS) -> List[str]:
    names = xls.sheet_names
    cands = set()
    for n in names:
        for p in patterns:
            if re.search(p, n):
                cands.add(n)
    return sorted(cands) if cands else names


# ==============================
# B-3. 코멘트 확장(셀 코멘트 + 비고열)
# ==============================
COMMENT_COL_CANDIDATES = ["비고", "notes", "comment", "코멘트"]

def _norm_header_list(cols: Iterable[Any]) -> List[str]:
    return [str(c).strip().lower() for c in cols]

def enrich_with_column_comments(
    xls: pd.ExcelFile,
    sheet_name: str,
    df_issues: pd.DataFrame,
    key_cols: List[str],
    comment_cols: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    df_issues: 기존 'Fail + 셀 코멘트' 추출 결과 (예상 컬럼: key_cols + ['comment_cell'])
    - key_cols 기준으로 동일 시트의 비고/Notes/Comment/코멘트 열을 병합하여 comment_text로 통합

    반환: df_issues에 'comment_text'와 'evidence_links' 컬럼 추가
    """
    if comment_cols is None:
        comment_cols = COMMENT_COL_CANDIDATES

    df_tbl = pd.read_excel(xls, sheet_name=sheet_name, engine="openpyxl")
    df_tbl.columns = _norm_header_list(df_tbl.columns)

    want_cols = list({*(c.lower() for c in comment_cols)} & set(df_tbl.columns))
    if want_cols:
        df_tbl["__comment_from_cols__"] = (
            df_tbl[want_cols].fillna("").astype(str).agg(" ".join, axis=1).str.strip()
        )
    else:
        df_tbl["__comment_from_cols__"] = ""

    # key_cols 정규화(헤더 케이스 차이 방지)
    key_norm = [k.lower() for k in key_cols]
    df_tbl = df_tbl.rename(columns={c: c.lower() for c in df_tbl.columns})
    df_issues = df_issues.rename(columns={c: c.lower() for c in df_issues.columns})

    merge_cols = [c for c in key_norm if c in df_tbl.columns and c in df_issues.columns]
    if not merge_cols:
        # 키가 안 맞으면 원본 반환하되, comment_text 최소한 보존
        df_issues["comment_text"] = df_issues.get("comment_cell", "").fillna("").astype(str).str.strip()
        df_issues["evidence_links"] = [[] for _ in range(len(df_issues))]
        return df_issues

    joined = df_issues.merge(
        df_tbl[merge_cols + ["__comment_from_cols__"]],
        on=merge_cols,
        how="left",
    )
    joined["comment_text"] = (
        (joined.get("comment_cell", "").fillna("").astype(str) + " " +
         joined["__comment_from_cols__"].fillna("").astype(str))
        .str.replace(r"\s+", " ", regex=True).str.strip()
    )

    # 간단한 URL 추출 → evidence_links
    urls = joined["comment_text"].str.extractall(r"(https?://\S+)")[0]
    link_map = urls.groupby(level=0).apply(list).to_dict()
    joined["evidence_links"] = [link_map.get(i, []) for i in range(len(joined))]

    return joined.drop(columns=["__comment_from_cols__"], errors="ignore")


# ==============================
# B-4. LLM JSON 강제 + Excel 리포트
# ==============================

JSON_SCHEMA_DOC = """
반드시 JSON만 출력. JSON 이외의 문자를 포함하지 말 것.
스키마:
{
  "summary": "string, 한 줄 총평(최대 150자).",
  "issues": [
    {
      "device_model": "string",
      "severity": "high|medium|low",
      "category": "성능|호환|안정성|UI|기타",
      "evidence": "string(증상/재현/로그/링크 요약)",
      "recommendation": "string(권장 액션)"
    }
  ],
  "device_risks": [
    {"device_model": "string", "reason": "string", "impact": "string"}
  ],
  "actions": [
    {"owner": "string", "due": "YYYY-MM-DD", "criteria": "string(검증 기준)"}
  ],
  "release_decision": "Go|No-Go|Conditional",
  "conditions": "string, 조건부일 때 조건(없으면 빈 문자열)"
}
"""

def build_system_prompt() -> str:
    return (
        "당신은 모바일/게임 QA 수석입니다. 데이터 기반, 과장 금지, 근거 우선, 한국어로 간결하게 작성하십시오.\n"
        + "이 대화에서는 반드시 JSON만 출력합니다. JSON 이외의 문자는 출력하지 마십시오.\n"
        + JSON_SCHEMA_DOC
    )

def build_user_prompt(metrics_summary: Dict[str, Any], sample_issues: pd.DataFrame, max_rows: int = 20) -> str:
    sample = sample_issues.head(max_rows).to_dict(orient="records")
    payload = {
        "metrics": metrics_summary,
        "sample_issues": sample
    }
    return f"다음 데이터를 바탕으로 JSON만 출력:\n{json.dumps(payload, ensure_ascii=False)}"

def parse_llm_json(text: str) -> Dict[str, Any]:
    """LLM 응답 문자열을 JSON으로 안전 파싱."""
    t = text.strip().strip("`").strip()
    try:
        return json.loads(t)
    except json.JSONDecodeError as e:
        # 흔한 오류 패턴 보정(앞뒤 잡다한 문구 제거 시도)
        first_brace = t.find("{")
        last_brace = t.rfind("}")
        if first_brace != -1 and last_brace != -1:
            return json.loads(t[first_brace:last_brace+1])
        raise e

def write_excel_report(result: Dict[str, Any], df_final: pd.DataFrame, path: str) -> None:
    """JSON 결과 + 근거 일부를 Excel로 출력(XlsxWriter)."""
    with pd.ExcelWriter(path, engine="xlsxwriter") as wr:
        # Summary
        pd.DataFrame([{
            "한 줄 총평": result.get("summary",""),
            "릴리스 권고": result.get("release_decision",""),
            "조건": result.get("conditions","")
        }]).to_excel(wr, sheet_name="Summary", index=False)

        # Issues
        pd.DataFrame(result.get("issues",[])).to_excel(wr, sheet_name="Issues", index=False)

        # Device Risks
        pd.DataFrame(result.get("device_risks",[])).to_excel(wr, sheet_name="Device_Risks", index=False)

        # Actions
        pd.DataFrame(result.get("actions",[])).to_excel(wr, sheet_name="Actions", index=False)

        # Evidence (원본 일부)
        cols = [c for c in ["test_item_id","device_model","result","comment_text","evidence_links"] if c in df_final.columns]
        if cols:
            df_final[cols].head(200).to_excel(wr, sheet_name="Evidence_Sample", index=False)
