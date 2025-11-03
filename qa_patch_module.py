# -*- coding: utf-8 -*-
"""
QA 통합 패치 모듈 (최종)
- 테스트 시트 자동 감지
- 셀 코멘트 + 비고/Notes 병합
- LLM JSON 강제(A~E 스펙, issues[] 제한 없음)
- Excel 리포트(Executive_Summary / Summary(상세 블록) / Issues / Device_Risks / Evidence_Sample / Cluster_*)
- 모듈 자가진단(self_check)
"""
import re
import json
import unicodedata
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
# 내부 유틸
# ==============================
def _nkfc(s: Any) -> str:
    if pd.isna(s): return ""
    s = str(s)
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[_\-\/(){}\[\]:+·∙•]", "", s)
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

# ==============================
# 코멘트 확장(셀 코멘트 + 비고열)
# ==============================
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
    fewshot = """
[예시-요약 스타일]
summary: "입력 지연·회전 레이아웃 이슈 다수. 고사양군 정상, 펀치홀/노치군 보완 필요."

[예시-issues 항목 1]
title: "회전 시 레이아웃 깨짐(펀치홀 계열)"
symptom: "세로→가로 전환 직후 탭바 겹침 및 터치 블록"
reproduction: "펀치홀 기기에서 홈→설정→가로 회전→탭 전환"
evidence: ["Galaxy S23 / Android14 주석", "로그 스크린샷 #12"]
impact: "탐색 불가 구간 발생"
priority: "P1"
cause: "cutout insets 미고려"
recommendation: "WindowInsets 적용 및 cutout 대응 규칙 추가"

[예시-issues 항목 2]
title: "입력 지연(중저가 GPU)"
symptom: "리스트 스크롤·터치 반응 지연"
reproduction: "저가 PowerVR 계열에서 리스트 스크롤 반복"
evidence: ["Redmi Note9, PowerVR GE8320", "프레임 드랍 로그"]
impact: "UX 저하"
priority: "P1"
cause: "과도한 overdraw / 미세 최적화 부재"
recommendation: "DiffUtil/RecyclerView 최적화 및 overdraw 감축"
"""
    return (
        "당신은 모바일 앱/게임 QA 리드입니다. 데이터 기반, 간결한 한국어로 작성하세요.\n"
        "입력(메트릭/변동/근거/표본) 내 사실만 사용하고 추측·과장은 금지합니다.\n"
        "조사형 표현(확인/분석/추정) 위주, 중복·장황 서술 지양.\n"
        "metrics.log_hypotheses가 있으면 이슈의 cause/evidence에 우선 반영.\n"
        "출력은 반드시 JSON이며 아래 스키마를 엄격 준수. (키 추가 금지)\n"
        + JSON_SCHEMA_DOC
        + "\n[형식 예시]\n" + fewshot.strip()
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
        "※ clusters_feature_detailed / by_issue_tag 를 활용해 GPU/CPU 외 공통 이슈도 요약하라.\n"
        "[형식] JSON { summary, issues[], device_risks[], actions[], release_decision, conditions }"
    )
    return (
        "다음 입력을 바탕으로 [지시]를 충실히 이행하고, 오직 JSON만 출력하십시오.\n"
        f"[입력]\n프로젝트: {project}, 체크리스트 버전: {version}\n"
        "메트릭: {총합패스율, 카테고리별 패스율, 상위 실패N, 고심각도 목록, 디바이스별 패스율}\n"
        "변동: {이전 릴리스 대비 변화}\n"
        "근거: {대표 실패 로그/링크}\n\n"
        + guideline + "\n\n=== 데이터(JSON) ===\n"
        + json.dumps(payload, ensure_ascii=False, separators=(",",":"))  # 공백 제거로 토큰 절감
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
# Summary 상세 블록 빌더
# ------------------------------
_DEVICE_PAT = re.compile(r"(Galaxy\s?[A-Z0-9\-]+|SM\-[A-Z0-9]+|iPhone\s?[A-Z0-9\-+ ]+|Pixel\s?\d+(?:\sPro)?|Redmi\s?[A-Z0-9\-]+|Xiaomi\s?[A-Z0-9\-]+|OPPO\s?[A-Z0-9\-]+|VIVO\s?[A-Z0-9\-]+)", re.I)

def _pick_devices_from_evidence(evidence_list, fallback_models=None, topn=3):
    devices = []
    for e in evidence_list or []:
        m = _DEVICE_PAT.findall(str(e) or "")
        for v in m:
            v = str(v).strip()
            if v and v not in devices:
                devices.append(v)
        if len(devices) >= topn:
            break
    if (not devices) and fallback_models:
        for v in fallback_models:
            v = str(v).strip()
            if v and v not in devices:
                devices.append(v)
            if len(devices) >= topn:
                break
    return ", ".join(devices[:topn]) if devices else ""

def build_summary_block(
    issues: List[Dict[str, Any]],
    topn: int = 100,
    metrics: Optional[Dict[str, Any]] = None,
) -> str:
    lines = []

    # [A] 일반 이슈
    for i, iss in enumerate(issues or [], start=1):
        if i > topn: break
        symp   = (iss.get("symptom") or "").strip()
        impact = (iss.get("impact") or "").strip()
        cause  = (iss.get("cause") or "").strip()
        rec    = (iss.get("recommendation") or "").strip()
        devices = _pick_devices_from_evidence(
            iss.get("evidence", []),
            fallback_models=iss.get("repr_models") or [],
            topn=3
        )
        block = "\n".join([
            f"* 현상: {symp or '(미기재)'}",
            f"* 발생 기기: {devices}",
            f"* 영향: {impact or '(미기재)'}",
            f"* 원인 추정: {cause or '(미기재)'}",
            f"* 권고: {rec or '(미기재)'}",
        ])
        lines.append(block.strip())
        lines.append("---")

    # [B] Feature 군집
    if isinstance(metrics, dict):
        fcd = metrics.get("clusters_feature_detailed") or []
        for c in fcd:
            tag  = (c.get("feature_tag") or "").strip()
            patt = (c.get("pattern") or tag or "공통 UI/디스플레이 이슈").strip()
            evidence_list = []
            for ev in c.get("evidence_rows", []):
                if isinstance(ev, dict):
                    evidence_list.append(ev.get("comment") or ev.get("device") or "")
                else:
                    evidence_list.append(str(ev))
            devices = _pick_devices_from_evidence(
                evidence_list,
                fallback_models=c.get("repr_models") or [],
                topn=3
            )
            title_map = {
                "punch_hole": "펀치홀 디바이스 공통",
                "notch": "노치 디바이스 공통",
                "rotation": "화면 회전 공통",
                "aspect_ratio": "화면비/레이아웃 공통",
                "resolution": "해상도/스케일링 공통",
                "cutout": "디스플레이 컷아웃 공통",
                "install": "설치/패키지 공통",
                "permission": "권한/퍼미션 공통",
                "login": "로그인/인증 공통",
                "input_lag": "입력/터치 지연 공통",
                "keyboard": "키보드/IME 공통",
                "ui_scaling": "UI 스케일링 공통",
                "render_artifact": "렌더링 아티팩트 공통",
                "black_screen": "검은 화면 공통",
                "white_screen": "하얀 화면 공통",
                "crash": "크래시 공통",
                "network": "네트워크/SSL 공통",
                "audio": "오디오 공통",
                "camera": "카메라 공통",
                "thermal": "써멀/발열 공통",
                "fps": "프레임 저하 공통",
            }
            title = title_map.get(tag, f"{tag} 공통 이슈")
            symp   = patt
            impact = "사용자 경험 저하"
            cause  = f"{title} 환경에서의 공통 처리/레이아웃 대응 부족 가능"
            rec    = f"{title} 대응 로직 보강 및 회귀 검증"
            block = "\n".join([
                f"{title}",
                f"* 현상: {symp}",
                f"* 발생 기기: {devices}",
                f"* 영향: {impact}",
                f"* 원인 추정: {cause}",
                f"* 권고: {rec}",
            ])
            lines.append(block.strip())
            lines.append("---")

    while lines and lines[-1] == "---":
        lines.pop()
    return "\n\n".join(lines).strip()

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
        # 0) Executive_Summary
        exec_rows = [{
            "A. 한 줄 총평": result.get("summary",""),
            "C. 디바이스 리스크": " / ".join([d.get("device_model_or_combo","") for d in (result.get("device_risks") or [])][:5]),
            "E. 릴리스 권고": f"{result.get('release_decision','')} / 조건: {result.get('conditions','')}"
        }]
        pd.DataFrame(exec_rows).to_excel(wr, sheet_name="Executive_Summary", index=False)

        # 1) Summary — 상세 블록
        summary_text = build_summary_block(
            result.get("issues", []),
            topn=100,
            metrics=result.get("metrics")
        )
        if not summary_text:
            summary_text = (
                f"릴리스 권고: {result.get('release_decision','')} / 조건: {result.get('conditions','')}\n"
                f"- 주요 패턴/군집/핵심 문제/우선순위/종합 의견은 Issues 및 Device_Risks를 참조하십시오."
            )
        pd.DataFrame([{"Summary & Insight": summary_text}]).to_excel(wr, sheet_name="Summary", index=False)

        # 2) Issues
        issues = pd.DataFrame(result.get("issues", []))
        if issues.empty:
            pd.DataFrame([{"title":"(없음)"}]).to_excel(wr, sheet_name="Issues", index=False)
        else:
            issues.to_excel(wr, sheet_name="Issues", index=False)

        # 3) Device_Risks
        risks = pd.DataFrame(result.get("device_risks", []))
        risks.to_excel(wr, sheet_name="Device_Risks", index=False)

        # 4) Evidence_Sample
        cols = [c for c in ["Sheet","Device(Model)","GPU","Chipset","RAM","OS","Rank","Checklist","comment_text"] if c in df_final.columns]
        if cols:
            disp = df_final[cols].head(200).copy()
            for c in disp.columns:
                if str(disp[c].dtype) == "object" or str(disp[c].dtype).startswith("category"):
                    disp[c] = disp[c].astype(str)
            disp.to_excel(wr, sheet_name="Evidence_Sample", index=False)
        else:
            pd.DataFrame().to_excel(wr, sheet_name="Evidence_Sample", index=False)

        # 5) Cluster_* 시트
        metrics_in_result = result.get("metrics", {}) if isinstance(result, dict) else {}
        clusters = metrics_in_result.get("clusters", {})
        if isinstance(clusters, dict) and clusters:
            for key, rows in clusters.items():
                try:
                    pd.DataFrame(rows).to_excel(wr, sheet_name=f"Cluster_{key}", index=False)
                except Exception:
                    pass

        by_issue_tag = metrics_in_result.get("by_issue_tag", [])
        if by_issue_tag:
            try:
                pd.DataFrame(by_issue_tag).to_excel(wr, sheet_name="Cluster_by_issue_tag", index=False)
            except Exception:
                pass

        clusters_feature = metrics_in_result.get("clusters_feature_detailed", [])
        if clusters_feature:
            try:
                pd.DataFrame(clusters_feature).to_excel(wr, sheet_name="Cluster_feature_detailed", index=False)
            except Exception:
                pass
