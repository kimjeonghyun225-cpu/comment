# -*- coding: utf-8 -*-
"""
QA 통합 패치 모듈 (최종)
- 테스트 시트 자동 감지
- Fail 셀 + 코멘트 추출 (라벨행→Fail열 세로추출, 병합셀 보정)
- 셀 코멘트 + 비고/Notes 병합
- LLM JSON 강제(A~E 스펙, issues[] 제한 없음)
- Excel 리포트(Executive_Summary / Summary / Issues / Device_Risks / Evidence_Sample / Cluster_*)
- 모듈 자가진단(self_check)
"""
import re
import json
import unicodedata
from typing import List, Dict, Any, Optional
import pandas as pd
import openpyxl

# ==============================
# 공통 유틸
# ==============================
def _norm_for_header(s: str) -> str:
    s = unicodedata.normalize("NFKC", str(s))
    s = re.sub(r"[\s\-\_/()\[\]{}:+·∙•]", "", s)
    return s.lower().strip()

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

def normalize_model_name_strict(s):
    if pd.isna(s): return ""
    s = str(s)
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"\b(64|128|256|512)\s*gb\b", "", s, flags=re.I)
    s = re.sub(r"\b(black|white|blue|red|green|gold|silver|골드|블랙|화이트|실버)\b", "", s, flags=re.I)
    s = re.sub(r"[\s\-_]+", "", s)
    return s.lower().strip()

# ==============================
# 병합셀 안전 읽기 + 라벨행 탐지
# ==============================
def read_merged(ws, r, c):
    """병합 셀을 고려해서 (r,c)의 실제 값을 안전하게 읽는다."""
    for rng in ws.merged_cells.ranges:
        if (rng.min_row <= r <= rng.max_row) and (rng.min_col <= c <= rng.max_col):
            return ws.cell(row=rng.min_row, column=rng.min_col).value
    return ws.cell(row=r, column=c).value

def find_row_by_labels(ws, labels, search_rows=30, search_cols=80) -> int:
    labels_norm = { _norm_for_header(x) for x in labels }
    max_r = min(search_rows, ws.max_row)
    max_c = min(search_cols, ws.max_column)
    for r in range(1, max_r + 1):
        for c in range(1, max_c + 1):
            v = read_merged(ws, r, c)
            if v and _norm_for_header(v) in labels_norm:
                return r
    return 0

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
# Fail + 코멘트 추출 (라벨행→Fail열)
# ==============================
def extract_comments_as_dataframe(wb, target_sheet_names):
    """
    Fail 셀과 셀 코멘트를 찾고...
    """
    extracted = []
    
    # 입력 검증
    if not wb:
        return pd.DataFrame(columns=[
            "Sheet","Device(Model)","GPU","Chipset","RAM","OS","Rank","Checklist","comment_cell","Comment(Text)"
        ])
    
    if not target_sheet_names:
        return pd.DataFrame(columns=[
            "Sheet","Device(Model)","GPU","Chipset","RAM","OS","Rank","Checklist","comment_cell","Comment(Text)"
        ])
    
    # 워크북의 실제 시트명 확인
    try:
        available_sheets = wb.sheetnames
    except AttributeError:
        available_sheets = getattr(wb, 'sheet_names', [])
    
    for sheet_name in target_sheet_names:
        # 시트 존재 여부 확인
        if sheet_name not in available_sheets:
            print(f"경고: 시트 '{sheet_name}'를 찾을 수 없습니다.")
            continue
        
        try:
            ws = wb[sheet_name]
        except Exception as e:
            print(f"시트 '{sheet_name}' 로드 실패: {e}")
            continue
        
        # 라벨 행 찾기
        header_rows = {
            "Model":   find_row_by_labels(ws, ["Model","Device","제품명","제품","모델명","모델","단말","단말명","기종"]),
            "GPU":     find_row_by_labels(ws, ["GPU","그래픽","그래픽칩","그래픽스","그래픽프로세서"]),
            "Chipset": find_row_by_labels(ws, ["Chipset","SoC","AP","CPU","Processor","칩셋"]),
            "RAM":     find_row_by_labels(ws, ["RAM","메모리"]),
            "OS":      find_row_by_labels(ws, ["OS Version","Android","iOS","OS","펌웨어","소프트웨어버전"]),
            "Rank":    find_row_by_labels(ws, ["Rating Grade?","Rank","등급"]),
        }

        # Fail 셀 찾기
        try:
            for row in ws.iter_rows():
                for cell in row:
                    val = cell.value
                    # Fail 셀이고 코멘트가 있는 경우만 처리
                    if (isinstance(val, str) and 
                        val.strip().lower() == "fail" and 
                        cell.comment):
                        
                        r, c = cell.row, cell.column
                        device_info = {
                            key: (read_merged(ws, rr, c) if rr > 0 else "")
                            for key, rr in header_rows.items()
                        }
                        
                        # Excel 안내문 제거
                        comment_text = (cell.comment.text or "").split(
                            "https://go.microsoft.com/fwlink/?linkid=870924.", 1
                        )[-1].strip()

                        extracted.append({
                            "Sheet": ws.title,
                            "Device(Model)": str(device_info.get("Model","") or "").strip(),
                            "GPU":     str(device_info.get("GPU","") or "").strip(),
                            "Chipset": str(device_info.get("Chipset","") or "").strip(),
                            "RAM":     str(device_info.get("RAM","") or "").strip(),
                            "OS":      str(device_info.get("OS","") or "").strip(),
                            "Rank":    str(device_info.get("Rank","") or "").strip(),
                            "Checklist": ws.title,
                            "comment_cell": comment_text,
                            "Comment(Text)": "",
                        })
        except Exception as e:
            print(f"시트 '{sheet_name}' 처리 중 오류: {e}")
            continue
    
    # 결과 반환
    if not extracted:
        return pd.DataFrame(columns=[
            "Sheet","Device(Model)","GPU","Chipset","RAM","OS","Rank","Checklist","comment_cell","Comment(Text)"
        ])
    
    return pd.DataFrame(extracted)
        ws = wb[sheet_name]

        header_rows = {
            "Model":   find_row_by_labels(ws, ["Model","Device","제품명","제품","모델명","모델","단말","단말명","기종"]),
            "GPU":     find_row_by_labels(ws, ["GPU","그래픽","그래픽칩","그래픽스","그래픽프로세서"]),
            "Chipset": find_row_by_labels(ws, ["Chipset","SoC","AP","CPU","Processor","칩셋"]),
            "RAM":     find_row_by_labels(ws, ["RAM","메모리"]),
            "OS":      find_row_by_labels(ws, ["OS Version","Android","iOS","OS","펌웨어","소프트웨어버전"]),
            "Rank":    find_row_by_labels(ws, ["Rating Grade?","Rank","등급"]),
        }

        for row in ws.iter_rows():
            for cell in row:
                val = cell.value
                if isinstance(val, str) and val.strip().lower() == "fail" and cell.comment:
                    r, c = cell.row, cell.column
                    device_info = {
                        key: (read_merged(ws, rr, c) if rr > 0 else "")
                        for key, rr in header_rows.items()
                    }
                    # Excel 안내문 제거
                    comment_text = (cell.comment.text or "").split(
                        "https://go.microsoft.com/fwlink/?linkid=870924.", 1
                    )[-1].strip()

                    extracted.append({
                        "Sheet": ws.title,
                        "Device(Model)": str(device_info.get("Model","") or "").strip(),
                        "GPU":     str(device_info.get("GPU","") or "").strip(),
                        "Chipset": str(device_info.get("Chipset","") or "").strip(),
                        "RAM":     str(device_info.get("RAM","") or "").strip(),
                        "OS":      str(device_info.get("OS","") or "").strip(),
                        "Rank":    str(device_info.get("Rank","") or "").strip(),
                        "Checklist": ws.title,  # 필요 시 별도 라벨 취합 가능
                        "comment_cell": comment_text,
                        "Comment(Text)": "",
                    })
    if not extracted:
        return pd.DataFrame(columns=[
            "Sheet","Device(Model)","GPU","Chipset","RAM","OS","Rank","Checklist","comment_cell","Comment(Text)"
        ])
    return pd.DataFrame(extracted)

# ==============================
# 비고/Notes 병합
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
스키마:{
  "summary":"string (≤150자).",
  "issues":[{"title":"string","symptom":"string","reproduction":"string","evidence":["string","..."],"impact":"string","priority":"P0|P1|P2","cause":"string","recommendation":"string"}],
  "device_risks":[{"device_model_or_combo":"string","reason":"string","impact":"string"}],
  "actions":[{"owner":"string","due":"YYYY-MM-DD","criteria":"string"}],
  "release_decision":"Go|No-Go|Conditional",
  "conditions":"string"
}
"""

def build_system_prompt() -> str:
    return (
        "당신은 모바일 앱/게임 QA 리드입니다. 데이터 기반, 간결한 한국어로 작성하세요.\n"
        "입력 내 사실만 사용하고 추측·과장은 금지합니다.\n"
        "조사형 표현 위주, 중복·장황 서술 지양.\n"
        "metrics.log_hypotheses 존재 시 cause/evidence에 우선 반영.\n"
        "출력은 반드시 JSON이며 아래 스키마를 엄격 준수.\n" + JSON_SCHEMA_DOC
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
        "[역할] QA 리드\n"
        "[지시]\n"
        "1) 한 줄 총평(≤150자)\n"
        "2) 핵심 이슈(증상/재현/근거/영향/우선순위)\n"
        "3) 디바이스 리스크(조합·원인 추정)\n"
        "4) 액션플랜(담당/기한/검증기준)\n"
        "5) 릴리스 권고(Go/No-Go/Conditional+조건)\n"
        "※ clusters_feature_detailed / by_issue_tag 를 활용해 GPU/CPU 외 공통 이슈도 요약."
    )
    return (
        "다음 입력을 바탕으로 [지시]를 충실히 이행하고, 오직 JSON만 출력하십시오.\n"
        f"[입력] 프로젝트: {project}, 체크리스트: {version}\n"
        + guideline + "\n\n=== 데이터(JSON) ===\n"
        + json.dumps(payload, ensure_ascii=False, separators=(",",":"))
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

def build_summary_block(issues: List[Dict[str, Any]], topn: int = 100, metrics: Optional[Dict[str, Any]] = None) -> str:
    lines = []
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
        lines.append(block.strip()); lines.append("---")

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
                evidence_list, fallback_models=c.get("repr_models") or [], topn=3
            )
            title_map = {
                "punch_hole":"펀치홀 디바이스 공통","notch":"노치 디바이스 공통","rotation":"화면 회전 공통",
                "aspect_ratio":"화면비/레이아웃 공통","resolution":"해상도/스케일링 공통","cutout":"디스플레이 컷아웃 공통",
                "install":"설치/패키지 공통","permission":"권한/퍼미션 공통","login":"로그인/인증 공통",
                "input_lag":"입력/터치 지연 공통","keyboard":"키보드/IME 공통","ui_scaling":"UI 스케일링 공통",
                "render_artifact":"렌더링 아티팩트 공통","black_screen":"검은 화면 공통","white_screen":"하얀 화면 공통",
                "crash":"크래시 공통","network":"네트워크/SSL 공통","audio":"오디오 공통","camera":"카메라 공통",
                "thermal":"써멀/발열 공통","fps":"프레임 저하 공통",
            }
            title = title_map.get(tag, f"{tag} 공통 이슈")
            block = "\n".join([
                f"{title}",
                f"* 현상: {patt}",
                f"* 발생 기기: {devices}",
                f"* 영향: 사용자 경험 저하",
                f"* 원인 추정: {title} 환경 공통 대응 부족 가능",
                f"* 권고: {title} 대응 로직 보강 및 회귀 검증",
            ])
            lines.append(block.strip()); lines.append("---")

    while lines and lines[-1] == "---":
        lines.pop()
    return "\n\n".join(lines).strip()

# ==============================
# Excel 리포트
# ==============================
def write_excel_report(result: Dict[str, Any], df_final: pd.DataFrame, path: str) -> None:
    try:
        import xlsxwriter
        engine = "xlsxwriter"
    except Exception:
        try:
            import openpyxl  # noqa
            engine = "openpyxl"
        except Exception:
            raise RuntimeError("엑셀 작성 엔진이 없습니다. `pip install xlsxwriter` 또는 `pip install openpyxl`")

    with pd.ExcelWriter(path, engine=engine) as wr:
        pd.DataFrame([{
            "A. 한 줄 총평": result.get("summary",""),
            "C. 디바이스 리스크": " / ".join([d.get("device_model_or_combo","") for d in (result.get("device_risks") or [])][:5]),
            "E. 릴리스 권고": f"{result.get('release_decision','')} / 조건: {result.get('conditions','')}"
        }]).to_excel(wr, sheet_name="Executive_Summary", index=False)

        summary_text = build_summary_block(result.get("issues", []), topn=100, metrics=result.get("metrics"))
        if not summary_text:
            summary_text = (
                f"릴리스 권고: {result.get('release_decision','')} / 조건: {result.get('conditions','')}\n"
                f"- 주요 패턴/군집/핵심 문제/우선순위/종합 의견은 Issues 및 Device_Risks를 참조하십시오."
            )
        pd.DataFrame([{"Summary & Insight": summary_text}]).to_excel(wr, sheet_name="Summary", index=False)

        issues = pd.DataFrame(result.get("issues", []))
        (issues if not issues.empty else pd.DataFrame([{"title":"(없음)"}])).to_excel(wr, sheet_name="Issues", index=False)

        pd.DataFrame(result.get("device_risks", [])).to_excel(wr, sheet_name="Device_Risks", index=False)

        cols = [c for c in ["Sheet","Device(Model)","GPU","Chipset","RAM","OS","Rank","Checklist","comment_text"] if c in df_final.columns]
        (df_final[cols].head(200) if cols else pd.DataFrame()).to_excel(wr, sheet_name="Evidence_Sample", index=False)

        metrics_in_result = result.get("metrics", {}) if isinstance(result, dict) else {}
        clusters = metrics_in_result.get("clusters", {})
        if isinstance(clusters, dict) and clusters:
            for key, rows in clusters.items():
                try: pd.DataFrame(rows).to_excel(wr, sheet_name=f"Cluster_{key}", index=False)
                except Exception: pass
        by_issue_tag = metrics_in_result.get("by_issue_tag", [])
        if by_issue_tag:
            try: pd.DataFrame(by_issue_tag).to_excel(wr, sheet_name="Cluster_by_issue_tag", index=False)
            except Exception: pass
        clusters_feature = metrics_in_result.get("clusters_feature_detailed", [])
        if clusters_feature:
            try: pd.DataFrame(clusters_feature).to_excel(wr, sheet_name="Cluster_feature_detailed", index=False)
            except Exception: pass
