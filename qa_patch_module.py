# -*- coding: utf-8 -*-
"""
QA 통합 패치 모듈 (최종)
- 테스트 시트 자동 감지
- Fail 셀 + 코멘트 추출 (라벨행→Fail열 세로추출, 병합셀 보정, 수식/스레드댓글 대응)
- 셀 코멘트 + 비고/Notes 병합
- LLM JSON 강제(A~E 스펙, issues[] 제한 없음)
- Excel 리포트(Executive_Summary / Summary / Issues / Device_Risks / Evidence_Sample / Cluster_*)
- 모듈 자가진단(self_check)
"""
import io
import re
import json
import zipfile
import unicodedata
import xml.etree.ElementTree as ET
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
    labels_norm = {_norm_for_header(x) for x in labels}
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
# 스레드 댓글(threadedComments) 파싱
# ==============================
def _sanitize_excel_comment(text: str) -> str:
    """Excel 스레드 댓글/안내문 머리말을 제거하고 사용자 본문만 남긴다."""
    s = str(text or "")
    lines = [ln for ln in s.splitlines()]
    if lines:
        if re.match(r"^\s*\[?\s*스레드\s*댓글[^\]]*\]?\s*$", lines[0], flags=re.I):
            lines = lines[1:]
        elif re.match(r"^\s*this\s+cell\s+contains\s+a\s+threaded\s+comment", lines[0], flags=re.I):
            lines = lines[1:]
    s = "\n".join(lines)
    s = s.replace("https://go.microsoft.com/fwlink/?linkid=870924.", "")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s).strip()
    return s

def load_threaded_comments_map_from_bytes(xlsx_bytes: bytes) -> dict:
    """
    .xlsx 바이트에서 스레드 댓글을 파싱해 {(sheet_name, cell_ref): "text"} 사전으로 반환.
    여러 댓글이 한 셀에 붙은 경우 ' | '로 이어 붙임.
    """
    mapping = {}  # key: (sheet_name, ref), value: list[str]
    try:
        with zipfile.ZipFile(io.BytesIO(xlsx_bytes)) as zf:
            # 1) workbook -> (rId -> target), (sheet name -> rId)
            rels_map = {}
            try:
                rels_xml = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
                rn = {"r": "http://schemas.openxmlformats.org/package/2006/relationships"}
                for rel in rels_xml.findall("Relationship", rn):
                    rels_map[rel.get("Id")] = rel.get("Target")
            except Exception:
                pass

            sheet_to_target = {}
            try:
                wb_xml = ET.fromstring(zf.read("xl/workbook.xml"))
                ns = {"ns": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
                      "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships"}
                for s in wb_xml.find("ns:sheets", ns).findall("ns:sheet", ns):
                    rid = s.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
                    name = s.get("name")
                    target = rels_map.get(rid, "")
                    if target.startswith("/"): target = target[1:]
                    sheet_to_target[name] = target  # e.g. worksheets/sheet1.xml
            except Exception:
                pass

            # 2) 각 시트 rels에서 threadedComments 링크 찾기 -> XML 파싱
            for sheet_name, sheet_target in sheet_to_target.items():
                if not sheet_target: continue
                base = sheet_target.rsplit("/", 1)[0] if "/" in sheet_target else ""
                rels_path = f"xl/{base}/_rels/{sheet_target.split('/')[-1]}.rels" if base else f"xl/_rels/{sheet_target}.rels"
                if rels_path not in zf.namelist():  # 시트에 rels가 없으면 continue
                    continue

                rels_xml = ET.fromstring(zf.read(rels_path))
                rn = {"r": "http://schemas.openxmlformats.org/package/2006/relationships"}
                for rel in rels_xml.findall("Relationship", rn):
                    typ = rel.get("Type") or ""
                    if typ.endswith("/2018/threadedcomments"):
                        target = rel.get("Target", "")
                        if target.startswith("/"): target = target[1:]
                        tc_path = f"xl/{target}" if not target.startswith("xl/") else target
                        if tc_path not in zf.namelist():  # 방어
                            continue
                        try:
                            tc_xml = ET.fromstring(zf.read(tc_path))
                            # threadedComment 노드마다 ref="A1" 등 셀 주소가 있음
                            for tcm in tc_xml.iter():
                                if tcm.tag.endswith("threadedComment"):
                                    cell_ref = tcm.get("ref")
                                    if not cell_ref:
                                        continue
                                    texts = []
                                    # 텍스트 노드 수집
                                    for node in tcm.iter():
                                        if node.text and node.tag.endswith(("t", "text")):
                                            texts.append(str(node.text))
                                    txt = " ".join([x.strip() for x in texts if x and x.strip()])
                                    if txt:
                                        mapping.setdefault((sheet_name, cell_ref), []).append(txt)
                        except Exception:
                            continue

        # 3) 리스트 -> 문자열 합치기 + 정리
        mapping = {k: _sanitize_excel_comment(" | ".join(v)) for k, v in mapping.items()}
    except Exception:
        mapping = {}
    return mapping

# ==============================
# Fail + 코멘트 추출 (라벨행→Fail열)
# ==============================
def extract_comments_as_dataframe_dual(wb_comments, wb_values, target_sheet_names, threaded_map: dict = None):
    """
    Fail 셀과 코멘트를 찾고, 해당 Fail "열(column)"에서
    Model/GPU/Chipset/RAM/OS/Rank 값을 라벨행 기준으로 수직 추출.
    병합셀도 보정하여 정확도 확보.
    threaded_map: {(sheet_name, "A1"): "threaded text"} 보강에 사용.
    """
    if threaded_map is None:
        threaded_map = {}

    extracted = []
    for sheet_name in target_sheet_names:
        if sheet_name not in wb_comments.sheetnames or sheet_name not in wb_values.sheetnames:
            continue
        ws_c = wb_comments[sheet_name]
        ws_v = wb_values[sheet_name]

        header_rows = {
            "Model":   find_row_by_labels(ws_c, ["Model","Device","제품명","제품","모델명","모델","단말","단말명","기종"]),
            "GPU":     find_row_by_labels(ws_c, ["GPU","그래픽","그래픽칩","그래픽스","그래픽프로세서"]),
            "Chipset": find_row_by_labels(ws_c, ["Chipset","SoC","AP","CPU","Processor","칩셋"]),
            "RAM":     find_row_by_labels(ws_c, ["RAM","메모리"]),
            "OS":      find_row_by_labels(ws_c, ["OS Version","Android","iOS","OS","펌웨어","소프트웨어버전"]),
            "Rank":    find_row_by_labels(ws_c, ["Rating Grade?","Rank","등급"]),
        }

        def _get_val(rr, cc):
            if rr <= 0: return ""
            v = read_merged(ws_v, rr, cc)
            if isinstance(v, (int, float)):
                v = str(int(v)) if isinstance(v, float) and v.is_integer() else str(v)
            if isinstance(v, str) and v.startswith("="):
                return ""
            return (str(v) if v is not None else "").strip()

        for row in ws_c.iter_rows():
            for cell in row:
                val = cell.value
                if isinstance(val, str) and re.fullmatch(r"\s*fail\s*", val, flags=re.I):
                    r, c = cell.row, cell.column

                    # 1) openpyxl Notes(구형 메모)
                    raw_comment = (getattr(cell, "comment", None).text if getattr(cell, "comment", None) else "")
                    ctext = _sanitize_excel_comment(raw_comment)

                    # 2) threadedComments 보강
                    if not ctext:
                        key = (sheet_name, cell.coordinate)  # (시트명, "A1")
                        if key in threaded_map:
                            ctext = _sanitize_excel_comment(threaded_map[key])

                    # 3) 여전히 없다면 스킵
                    if not ctext:
                        continue

                    device_info = { key: _get_val(rr, c) for key, rr in header_rows.items() }

                    extracted.append({
                        "Sheet": ws_c.title,
                        "Device(Model)": device_info.get("Model",""),
                        "GPU":     device_info.get("GPU",""),
                        "Chipset": device_info.get("Chipset",""),
                        "RAM":     device_info.get("RAM",""),
                        "OS":      device_info.get("OS",""),
                        "Rank":    device_info.get("Rank",""),
                        "Checklist": ws_c.title,
                        "comment_cell": ctext,
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
# LLM JSON 강제
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
        "[역할] 모바일 앱/게임 QA 리드\n"
        "[지시]\n"
        "1) 한 줄 총평(≤300자)\n"
        "2) GPU/Chipset 군집 요약: metrics.clusters.by_gpu / by_chipset 과 공통 이슈내용을 활용하여\n"
        "   - 각 군집별 발생 건수와 특징(대표 증상)·권고를 1~2문장씩 요약\n"
        "   - 대표 증상은 sample_issues와 metrics.by_issue_tag(전역 태그 빈도)를 근거로 서술\n"
        "   - 군집 관련 이슈가 미미하면 생략 가능\n"
        "   - 선택적으로 JSON 키 cluster_gpu_summary[], cluster_chipset_summary[]에 문장 배열로 포함\n"
        "3) 공통 Feature 군집(펀치홀/노치/회전/입력지연/렌더 아티팩트 등) 요약: metrics.clusters_feature_detailed 활용\n"
        "4) 핵심 이슈(증상/재현/근거/영향/우선순위/원인·권고)\n"
        "5) 디바이스 리스크(조합·원인 추정)\n"
        "6) 액션플랜(담당/기한/검증기준)\n"
        "7) 릴리스 권고(Go/No-Go/Conditional+조건)\n"
        "[원칙] 과장 금지, 데이터 근거 기반, 중복 지양. 출력은 JSON만."
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
# Summary 상세 블록
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
    llm_cluster: Optional[Dict[str, Any]] = None,
) -> str:
    lines: List[str] = []

    # [A] 개별 이슈 요약 (기존 동작 유지)
    for i, iss in enumerate(issues or [], start=1):
        if i > topn: break
        symp   = (iss.get("symptom") or "").strip()
        impact = (iss.get("impact") or "").strip()
        cause  = (iss.get("cause") or "").strip()
        rec    = (iss.get("recommendation") or "").strip()

        ev_list = iss.get("evidence", []) or []
        repr_models = iss.get("repr_models") or []
        devices = _pick_devices_from_evidence(ev_list, fallback_models=repr_models, topn=3) if (ev_list or repr_models) else ""

        block = "\n".join([
            f"* 현상: {symp or '(미기재)'}",
            f"* 발생 기기: {devices}",
            f"* 영향: {impact or '(미기재)'}",
            f"* 원인 추정: {cause or '(미기재)'}",
            f"* 권고: {rec or '(미기재)'}",
        ])
        lines.append(block.strip())
        lines.append("---")

    # ===== [B] GPU/Chipset 군집 요약 (LLM 우선, 없으면 로컬 백필) =====
    def _append_section(title: str, bullets: List[str]):
        if not bullets: return
        lines.append(title)
        for b in bullets:
            b = str(b).strip()
            if b:
                lines.append(f"- {b}")
        lines.append("---")

    # LLM이 제공했으면 우선 사용
    llm_gpu = []
    llm_chip = []
    if isinstance(llm_cluster, dict):
        v1 = llm_cluster.get("cluster_gpu_summary")
        v2 = llm_cluster.get("cluster_chipset_summary")
        if isinstance(v1, list): llm_gpu = [str(x).strip() for x in v1 if str(x).strip()]
        if isinstance(v2, list): llm_chip = [str(x).strip() for x in v2 if str(x).strip()]

    # 로컬 백필 준비
    def _topn_list(rows, n=3):
        arr = []
        for r in rows or []:
            if not isinstance(r, dict): continue
            key = str(r.get("GPU") or r.get("Chipset") or "").strip()
            cnt = int(r.get("count") or 0)
            if key:
                arr.append((key, cnt))
        arr.sort(key=lambda x: x[1], reverse=True)
        return arr[:n]

    def _should_show_cluster(by_rows, total_issues, by_issue_tag):
        # 군집 요약이 '필요한' 경우만 표시: 하드웨어 연관 태그 or 비중 임계치
        if not total_issues or total_issues <= 0: return False
        hw_tags = {
            "fps","thermal","render_artifact","black_screen","white_screen",
            "crash","input_lag","ui_scaling","resolution","aspect_ratio",
            "audio","camera","network"
        }
        tag_hit = False
        try:
            for row in (by_issue_tag or []):
                val = str(row.get("value","")).strip().lower()
                cnt = int(row.get("count") or 0)
                if cnt >= 2 and val in hw_tags:
                    tag_hit = True; break
        except Exception:
            pass

        dominant = False
        try:
            top = _topn_list(by_rows, n=1)
            if top:
                _, top_cnt = top[0]
                if top_cnt >= 3 or (top_cnt / max(1, total_issues)) >= 0.25:
                    dominant = True
        except Exception:
            pass

        return tag_hit or dominant

    total_issues = int((metrics or {}).get("total_fail_issues") or 0)
    by_issue_tag = (metrics or {}).get("by_issue_tag") or []
    clusters = (metrics or {}).get("clusters") or {}
    by_gpu_rows = clusters.get("by_gpu") or []
    by_chip_rows = clusters.get("by_chipset") or []

    gpu_bullets, chip_bullets = [], []

    # GPU
    if llm_gpu:
        gpu_bullets = llm_gpu
    else:
        if _should_show_cluster(by_gpu_rows, total_issues, by_issue_tag):
            tops = _topn_list(by_gpu_rows, n=3)
            top_tags = sorted(by_issue_tag, key=lambda x: int(x.get("count",0)), reverse=True)[:3]
            tag_phrase = "/".join([str(t.get("value","")) for t in top_tags if str(t.get("value","")).strip()])
            for name, cnt in tops:
                gpu_bullets.append(f"{name}: {cnt}건 (대표 증상: {tag_phrase or '디스플레이/성능/안정성'})")

    # Chipset
    if llm_chip:
        chip_bullets = llm_chip
    else:
        if _should_show_cluster(by_chip_rows, total_issues, by_issue_tag):
            tops = _topn_list(by_chip_rows, n=3)
            top_tags = sorted(by_issue_tag, key=lambda x: int(x.get("count",0)), reverse=True)[:3]
            tag_phrase = "/".join([str(t.get("value","")) for t in top_tags if str(t.get("value","")).strip()])
            for name, cnt in tops:
                chip_bullets.append(f"{name}: {cnt}건 (대표 증상: {tag_phrase or '디스플레이/성능/안정성'})")

    _append_section("GPU 군집 요약", gpu_bullets)
    _append_section("Chipset 군집 요약", chip_bullets)

    # ===== [C] Feature 군집 요약 (기존 유지) =====
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
