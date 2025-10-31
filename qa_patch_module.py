# -*- coding: utf-8 -*-
"""
QA 통합 패치 모듈 (최종)
- 테스트 시트 자동 감지
- 셀 코멘트 + 비고/Notes 병합
- LLM JSON 강제(A~E 스펙, issues[] 제한 없음)
- Excel 리포트(Executive_Summary / Summary(상세 블록) / Issues / Device_Risks / Evidence_Sample / Cluster_* (있으면))
- 모듈 자가진단(self_check)
"""

import re
import json
from typing import List, Dict, Any, Optional
import pandas as pd

# ==============================
# 테스트 시트 자동 감지
# ==============================
def find_test_sheet_candidates(xls) -> list:
    names = [str(n) for n in getattr(xls, "sheet_names", [])]
    patterns = [
        r"(?i)\btest\s*case\b.*\b(aos|android)\b",
        r"(?i)\btest\s*case\b.*\b(ios)\b",
        r"(?i)\btestcase(?:[ _\-]*)aos\b",
        r"(?i)\btestcase(?:[ _\-]*)ios\b",
        r"(?i)\bcompatibility\s*test\b.*\b(aos|android)\b",
        r"(?i)\bcompatibility\s*test\b.*\b(ios)\b",
        r"(?i)호환성\s*테스트.*(aos|android|ios)",
        r"(?i)compatibility\s*test\((?:aos|ios)\)",
        r"(?i)\btc[_\- ]?(aos|android)\b",
        r"(?i)\btc[_\- ]?ios\b",
        r"(?i)\bcompat[_\- ]?test[_\- ]?[a-z]?\b",
    ]
    cands = set()
    for n in names:
        for p in patterns:
            try:
                if re.search(p, n):
                    cands.add(n); break
            except re.error:
                continue
    if not cands:
        for n in names:
            norm = re.sub(r"[\s_\-]+", "", n.lower())
            if any(k in norm for k in ["testcase","compatibilitytest","테스트","호환성","tc_","tc-","tc "]):
                cands.add(n)
    return sorted(cands) if cands else names

# ==============================
# 코멘트 확장(셀 코멘트 + 비고열)
# ==============================
def _nkfc(s: Any) -> str:
    if pd.isna(s): return ""
    import re as _re
    s = str(s)
    s = _re.sub(r"\s+", "", s)
    s = _re.sub(r"[_\-\/(){}\[\]:+·∙•]", "", s)
    return s.strip().lower()

def _safe_series(df: pd.DataFrame, col: str) -> pd.Series:
    return df[col] if col in df.columns else pd.Series([""] * len(df), index=df.index)

def _pick_first_nonempty(*series):
    out = pd.Series([""] * len(series[0]), index=series[0].index, dtype="object")
    for s in series:
        s2 = s.fillna("").astype(str)
        mask = (out == "") & (s2.str.len() > 0)
        out.loc[mask] = s2.loc[mask]
    return out

def enrich_with_column_comments(
    xls,
    test_sheet_name: str,
    df_issues: pd.DataFrame,
    key_cols: Optional[List[str]] = None,
) -> pd.DataFrame:
    if key_cols is None:
        key_cols = ["Checklist", "Device(Model)"]

    issues = df_issues.copy()
    base_comment = _pick_first_nonempty(
        _safe_series(issues, "comment_cell"),
        _safe_series(issues, "Comment(Text)"),
    )
    issues["comment_text"] = base_comment.fillna("").astype(str)
    if "evidence_links" not in issues.columns:
        issues["evidence_links"] = [[] for _ in range(len(issues))]

    try:
        df_tbl = pd.read_excel(xls, sheet_name=test_sheet_name, engine="openpyxl")
    except Exception:
        return issues
    if df_tbl is None or df_tbl.empty:
        return issues

    note_candidates = [
        c for c in df_tbl.columns
        if str(c).strip().lower() in {"notes","note","비고","comment","comments","코멘트"}
    ]
    if not note_candidates:
        return issues

    issues_keys = []
    for k in key_cols:
        if (k in issues.columns) and (k in df_tbl.columns):
            issues[f"__key_{k}__"] = issues[k].map(_nkfc)
            df_tbl[f"__key_{k}__"]  = df_tbl[k].map(_nkfc)
            issues_keys.append(f"__key_{k}__")

    if not issues_keys:
        return issues

    note_df = df_tbl[issues_keys + note_candidates].copy()
    for c in note_candidates:
        note_df[c] = note_df[c].astype(str).fillna("")
    agg_map = {c: lambda s: " / ".join([x for x in s if x and x.lower() != "nan"]) for c in note_candidates}
    note_df = note_df.groupby(issues_keys, as_index=False).agg(agg_map)

    merged = pd.merge(issues, note_df, how="left", left_on=issues_keys, right_on=issues_keys)

    def _row_notes(row):
        vals = []
        for c in note_candidates:
            v = row.get(c, "")
            if isinstance(v, str) and v and v.lower() != "nan":
                vals.append(v.strip())
        return " | ".join(vals)

    notes_joined = merged.apply(_row_notes, axis=1)
    merged["comment_text"] = (
        merged["comment_text"].astype(str).str.strip()
        + ((" | " + notes_joined).where(notes_joined.str.len() > 0, ""))
    ).str.strip(" |")

    if "evidence_links" not in merged.columns:
        merged["evidence_links"] = [[] for _ in range(len(merged))]

    return merged

# ==============================
# 모듈 자가진단
# ==============================
def self_check(df_issues: pd.DataFrame) -> Dict[str, Any]:
    checks = []
    for col in ["Device(Model)", "Checklist"]:
        checks.append((col, col in df_issues.columns))
    comment_text_ok = "comment_text" in df_issues.columns
    row_ok = (df_issues is not None) and (len(df_issues) > 0)
    return {
        "columns_ok": all(flag for _, flag in checks),
        "comment_text_ok": comment_text_ok,
        "row_ok": row_ok,
        "detail": checks,
        "rows": 0 if df_issues is None else len(df_issues),
    }

# ==============================
# LLM JSON 강제 (A~E 스펙, issues[] 제한 없음)
# ==============================
JSON_SCHEMA_DOC = r"""
반드시 JSON만 출력. JSON 이외의 문자를 포함하지 말 것.
스키마:
{
  "summary": "string (≤150자).",
  "issues": [
    {
      "title": "string",
      "symptom": "string",
      "reproduction": "string",
      "evidence": ["string","..."],
      "impact": "string",
      "priority": "P0|P1|P2",
      "cause": "string",
      "recommendation": "string"
    }
  ],
  "device_risks": [
    {"device_model_or_combo":"string","reason":"string","impact":"string"}
  ],
  "actions": [
    {"owner":"string","due":"YYYY-MM-DD","criteria":"string"}
  ],
  "release_decision": "Go|No-Go|Conditional",
  "conditions": "string"
}
"""

def build_system_prompt() -> str:
    return (
        "당신은 모바일 앱/게임 QA 리드입니다. 데이터 기반, 간결한 한국어로 작성하세요.\n"
        "입력(메트릭/변동/근거/표본) 내 사실만 사용하고 추측·과장은 금지합니다.\n"
        "조사형 표현(확인/분석/추정됩니다)을 사용하고, 중복·장황 서술을 지양합니다.\n"
        "데이터 기반: 제공된 증거만 사용, 추측 금지\n"
        "metrics.log_hypotheses 가 존재하면, 각 이슈의 'cause'와 근거(evidence)에 우선 반영하십시오.\n"
        "출력은 반드시 JSON이며, 다음 스키마를 따르세요.\n"
        + JSON_SCHEMA_DOC
    )

def build_user_prompt(
    project: str,
    version: str,
    metrics: Dict[str, Any],
    deltas: Dict[str, Any],
    evidence_links: List[str],
    sample_issues: pd.DataFrame,
    max_rows: int = 200
) -> str:
    sample = sample_issues.head(max_rows).to_dict(orient="records")
    payload = {
        "project": project,
        "checklist_version": version,
        "metrics": metrics,
        "deltas": deltas,
        "evidence": evidence_links,
        "sample_issues": sample
    }
    guideline = (
        "[역할] 당신은 모바일 앱/게임 QA 리드입니다. 데이터 기반, 간결한 한국어로 작성.\n"
        "[지시]\n"
        "1) 한 줄 총평(≤150자)\n"
        "2) 핵심 이슈: (증상/재현/근거/영향/우선순위)  # 개수 제한 없음\n"
        "3) 디바이스 리스크(조합·원인 추정)\n"
        "4) 액션플랜(담당/기한/검증기준)\n"
        "5) 릴리스 권고(Go/No-Go/Conditional + 조건)\n"
        "[형식] JSON { summary, issues[], device_risks[], actions[], release_decision, conditions }"
    )
    return (
        "다음 입력을 바탕으로 [지시]를 충실히 이행하고, 오직 JSON만 출력하십시오.\n"
        f"[입력]\n프로젝트: {project}, 체크리스트 버전: {version}\n"
        "메트릭: {총합패스율, 카테고리별 패스율, 상위 실패N, 고심각도 목록, 디바이스별 패스율}\n"
        "변동: {이전 릴리스 대비 변화}\n"
        "근거: {대표 실패 로그/링크}\n\n"
        + guideline + "\n\n=== 데이터(JSON) ===\n"
        + json.dumps(payload, ensure_ascii=False)
    )

def parse_llm_json(text: str) -> Dict[str, Any]:
    t = text.strip().strip("`").strip()
    try:
        obj = json.loads(t)
    except Exception as e:
        first, last = t.find("{"), t.rfind("}")
        if first != -1 and last != -1:
            obj = json.loads(t[first:last+1])
        else:
            raise ValueError(f"LLM JSON 파싱 실패: {e}") from e
    required = ["summary","issues","device_risks","release_decision","conditions"]
    missing = [k for k in required if k not in obj]
    if missing:
        raise ValueError(f"LLM 결과에 필수 키 누락: {missing}")
    obj.setdefault("actions", [])
    return obj

# ------------------------------
# Summary 상세 블록 빌더 (현상/기기/영향/원인 추정/권고)
# ------------------------------
def build_summary_block(issues: List[Dict[str, Any]], topn: int = 3) -> str:
    lines = []
    for i, iss in enumerate(issues or [], start=1):
        if i > topn: break
        title  = iss.get("title","이슈")
        symp   = iss.get("symptom","")
        impact = iss.get("impact","")
        cause  = iss.get("cause","(추정 근거 부족)")
        rec    = iss.get("recommendation","")
        # 발생 기기(간단 추출: evidence에서 모델명 포함 라인 사용)
        dev_line = ""
        for e in iss.get("evidence",[]) or []:
            if any(k in str(e) for k in ["Galaxy","Xiaomi","iPhone","OPPO","VIVO","Redmi","Pixel","SM-"]):
                dev_line = str(e); break
        if not dev_line and (iss.get("evidence") or []):
            dev_line = str(iss["evidence"][0])
        block = (
            f"{title}\n"
            f"* 현상: {symp}\n"
            f"* 발생 기기: {dev_line}\n"
            f"* 영향: {impact}\n"
            f"* 원인 추정: {cause}\n"
            f"* 권고: {rec}"
        )
        lines.append(block.strip())
    return ("\n\n---\n\n".join(lines)).strip()

def write_excel_report(result: Dict[str, Any], df_final: pd.DataFrame, path: str) -> None:
    # 엔진 자동 선택
    try:
        import xlsxwriter  # noqa
        engine = "xlsxwriter"
    except Exception:
        try:
            import openpyxl  # noqa
            engine = "openpyxl"
        except Exception:
            raise RuntimeError("엑셀 작성 엔진이 없습니다. `pip install xlsxwriter` 또는 `pip install openpyxl`")

    with pd.ExcelWriter(path, engine=engine) as wr:
        # 0) Executive_Summary (요약표: A/C/E 중심)
        exec_rows = [{
            "A. 한 줄 총평": result.get("summary",""),
            "C. 디바이스 리스크": " / ".join([d.get("device_model_or_combo","") for d in (result.get("device_risks") or [])][:5]),
            "E. 릴리스 권고": f"{result.get('release_decision','')} / 조건: {result.get('conditions','')}"
        }]
        pd.DataFrame(exec_rows).to_excel(wr, sheet_name="Executive_Summary", index=False)

        # 1) Summary — 상세 블록(현상/기기/영향/원인 추정/권고)
        summary_text = build_summary_block(result.get("issues", []), topn=100)
        if not summary_text:
            summary_text = (
                f"릴리스 권고: {result.get('release_decision','')} / 조건: {result.get('conditions','')}\n"
                f"- 주요 패턴/군집/핵심 문제/우선순위/종합 의견은 Issues 및 Device_Risks를 참조하십시오."
            )
        pd.DataFrame([{"Summary & Insight": summary_text}]).to_excel(wr, sheet_name="Summary", index=False)

        # 2) Issues (제한 없음)
        issues = pd.DataFrame(result.get("issues", []))
        if issues.empty:
            pd.DataFrame([{"title":"(없음)"}]).to_excel(wr, sheet_name="Issues", index=False)
        else:
            issues.to_excel(wr, sheet_name="Issues", index=False)

        # 3) Device_Risks
        risks = pd.DataFrame(result.get("device_risks", []))
        risks.to_excel(wr, sheet_name="Device_Risks", index=False)

        # 4) Evidence_Sample(원본 일부)
        cols = [c for c in ["Sheet","Device(Model)","GPU","Chipset","RAM","OS","Rank","Checklist","comment_text"] if c in df_final.columns]
        if cols:
            disp = df_final[cols].head(200).copy()
            for c in disp.columns:
                if str(disp[c].dtype) == "object" or str(disp[c].dtype).startswith("category"):
                    disp[c] = disp[c].astype(str)
            disp.to_excel(wr, sheet_name="Evidence_Sample", index=False)
        else:
            pd.DataFrame().to_excel(wr, sheet_name="Evidence_Sample", index=False)

        # (선택) 5) Cluster_* 시트: result.metrics.clusters 가 있으면 기록
        metrics_in_result = result.get("metrics", {})
        clusters = metrics_in_result.get("clusters", {}) if isinstance(metrics_in_result, dict) else {}
        if isinstance(clusters, dict) and clusters:
            for key, rows in clusters.items():
                try:
                    pd.DataFrame(rows).to_excel(wr, sheet_name=f"Cluster_{key}", index=False)
                except Exception:
                    pass
