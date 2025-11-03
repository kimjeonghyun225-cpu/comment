# -*- coding: utf-8 -*-
# comment1.py — 최종본
# 목표: 코멘트 품질 최우선 / 군집 단위 GPT 미니 프롬프트 / qa_patch_module 의존 제거

import os
import re
import io
import time
import zipfile
import unicodedata
from contextlib import contextmanager
from collections import defaultdict

import pandas as pd
import openpyxl
import streamlit as st
from dotenv import load_dotenv
from openai import OpenAI

# =========================
# 환경설정
# =========================
load_dotenv()
api_key = st.secrets.get("OPENAI_API_KEY", os.getenv("OPENAI_API_KEY", ""))
if not api_key:
    st.error("OpenAI API 키가 없습니다. st.secrets 또는 .env에 OPENAI_API_KEY를 설정하세요.")
    st.stop()
client = OpenAI(api_key=api_key)

st.set_page_config(page_title="QA 결과 자동 코멘트 생성기", layout="wide")
st.title(":bar_chart: QA 결과 자동 코멘트 생성기")

# 세션 초기화 버튼(프로젝트 간 잔여 상태 제거)
if st.button("🔄 세션 초기화(모든 내부 상태 리셋)"):
    st.session_state.clear()
    st.rerun()

# =========================
# 공통 유틸
# =========================
@contextmanager
def step_status(title: str):
    with st.status(title, expanded=False) as status:
        t0 = time.time()
        try:
            yield status
            status.update(label=f"{title} - 완료 ({time.time()-t0:.2f}s)", state="complete", expanded=False)
        except Exception as e:
            status.update(label=f"{title} - 실패: {e}", state="error", expanded=True)
            raise

def diag_dump(label: str, obj):
    with st.expander(f"🔎 진단 보기: {label}", expanded=False):
        st.write(obj)

def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKC", str(s))
    s = re.sub(r"[\s\-\_/()\[\]{}:+·∙•]", "", s)
    return s.lower().strip()

def normalize_model_name_strict(s):
    if pd.isna(s): return ""
    s = str(s)
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"\b(64|128|256|512)\s*gb\b", "", s, flags=re.I)
    s = re.sub(r"\b(black|white|blue|red|green|gold|silver|골드|블랙|화이트|실버)\b", "", s, flags=re.I)
    s = re.sub(r"[\s\-_]+", "", s)
    return s.lower().strip()

# =========================
# 테스트 시트 자동 감지
# =========================
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

# =========================
# Fail + 셀 코멘트 추출 + 비고/Notes 병합
# =========================
def find_row_by_labels(ws, labels, search_rows=30, search_cols=70):
    max_r = min(search_rows, ws.max_row)
    max_c = min(search_cols, ws.max_column)
    target = set(str(x).strip() for x in labels)
    for r in range(1, max_r + 1):
        for c in range(1, max_c + 1):
            v = ws.cell(row=r, column=c).value
            if v and str(v).strip() in target:
                return r
    return 0

def get_checklist_label(ws, row):
    label_parts, columns_to_check = [], [6, 7, 9]
    for c in columns_to_check:
        for r_search in range(row, 0, -1):
            cell_value = ws.cell(row=r_search, column=c).value
            if cell_value and str(cell_value).strip():
                label_parts.append(str(cell_value).replace("\n", " ").strip())
                break
    return " / ".join(label_parts)

def extract_comments_as_dataframe(wb, target_sheet_names):
    extracted = []
    for sheet_name in target_sheet_names:
        if sheet_name not in wb.sheetnames:
            continue
        ws = wb[sheet_name]
        header_rows = {
            "Model":   find_row_by_labels(ws, ["Model", "제품명"]),
            "Chipset": find_row_by_labels(ws, ["Chipset", "CPU", "AP"]),
            "RAM":     find_row_by_labels(ws, ["RAM", "메모리"]),
            "Rank":    find_row_by_labels(ws, ["Rating Grade?", "Rank", "등급"]),
            "OS":      find_row_by_labels(ws, ["OS Version", "Android", "iOS", "OS"]),
        }
        for row in ws.iter_rows():
            for cell in row:
                val = cell.value
                if isinstance(val, str) and val.strip().lower() == "fail" and cell.comment:
                    r, c = cell.row, cell.column
                    device_info = {
                        key: ws.cell(row=num, column=c).value if num > 0 else ""
                        for key, num in header_rows.items()
                    }
                    checklist = get_checklist_label(ws, r)
                    comment_text = (cell.comment.text or "").split(
                        "https://go.microsoft.com/fwlink/?linkid=870924.", 1
                    )[-1].strip()
                    extracted.append({
                        "Sheet": ws.title,
                        "Device(Model)": device_info.get("Model", ""),
                        "Chipset": device_info.get("Chipset", ""),
                        "RAM": device_info.get("RAM", ""),
                        "Rank": device_info.get("Rank", ""),
                        "OS": device_info.get("OS", ""),
                        "Checklist": checklist,
                        "comment_cell": comment_text,
                        "Comment(Text)": "",
                    })
    if not extracted:
        return None
    return pd.DataFrame(extracted)

def _nkfc(s: str) -> str:
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

def enrich_with_column_comments(xls, test_sheet_name: str, df_issues: pd.DataFrame) -> pd.DataFrame:
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
    for k in ["Checklist", "Device(Model)"]:
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

# =========================
# 스펙 시트 병합
# =========================
def _norm_for_header(s: str) -> str:
    s = unicodedata.normalize("NFKC", str(s))
    s = re.sub(r"[\s\-\_/()\[\]{}:+·∙•]", "", s)
    return s.lower().strip()

def load_std_spec_df(xls, sheet):
    df_probe = pd.read_excel(xls, sheet_name=sheet, header=None, engine="openpyxl")
    header_row_idx = 0
    header_candidates = [r"^model$", r"^device$", r"^제품명$", r"^제품$", r"^모델명$", r"^모델$"]
    for r in range(min(12, len(df_probe))):
        row_vals = df_probe.iloc[r].astype(str).fillna("")
        norm_vals = [_norm_for_header(v) for v in row_vals]
        for v in norm_vals:
            if any(re.search(pat, v) for pat in header_candidates):
                header_row_idx = r; break
        if header_row_idx:
            break

    df = pd.read_excel(xls, sheet_name=sheet, header=header_row_idx, engine="openpyxl")
    original_cols = list(df.columns)
    norm_cols = [_norm_for_header(c) for c in original_cols]
    synonyms = {
        r"^(model|device|제품명|제품|모델명|모델)$": "Model",
        r"^(maker|manufacturer|brand|oem|제조사|벤더)$": "제조사",
        r"^(gpu|그래픽|그래픽칩|그래픽스|그래픽프로세서)$": "GPU",
        r"^(chipset|soc|ap|cpu)$": "Chipset",
        r"^(ram|메모리)$": "RAM",
        r"^(os|osversion|android|ios|펌웨어|소프트웨어버전)$": "OS",
        r"^(rank|rating|ratinggrade|등급)$": "Rank",
    }
    col_map = {}
    for norm_name, orig_name in zip(norm_cols, original_cols):
        mapped = None
        for pat, std_name in synonyms.items():
            if re.search(pat, norm_name):
                mapped = std_name; break
        col_map[orig_name] = mapped or orig_name
    df = df.rename(columns=col_map)

    def _normalize_model(s):
        if pd.isna(s): return ""
        s = str(s)
        s = re.sub(r"\(.*?\)", "", s)
        s = re.sub(r"[\s\-_]+", "", s)
        return s.lower().strip()

    model_col = "Model" if "Model" in df.columns else None
    if model_col is None:
        for c in df.columns:
            if re.search(r"^(model|device|제품명|제품|모델명|모델)$", _norm_for_header(c)):
                model_col = c; break
    if model_col is None:
        raise ValueError(f"'{sheet}'에서 모델 컬럼을 찾지 못했습니다. 컬럼: {list(df.columns)}")

    df["model_norm"] = df[model_col].apply(_normalize_model)
    cols_keep = ["model_norm"]
    for c in ["GPU","제조사","Chipset","RAM","OS","Rank","Model"]:
        if c in df.columns:
            cols_keep.append(c)
    return df[cols_keep]

# =========================
# 로그 요약(선택) & 근본원인 추정
# =========================
def load_and_summarize_logcat_files(files):
    patterns = {
        "crash": re.compile(r"\bFATAL EXCEPTION\b|\bAbort message:\b|\bbacktrace\b", re.I),
        "anr": re.compile(r"\bANR in\b|\bApplication Not Responding\b", re.I),
        "gl_err": re.compile(r"(E/libEGL|E/GLConsumer|OpenGLRenderer|Adreno|Mali)", re.I),
        "thermal": re.compile(r"(thermal|ThermalEngine|throttl)", re.I),
        "net": re.compile(r"(SocketTimeout|UnknownHost|SSLHandshake|Network is unreachable)", re.I),
        "fps": re.compile(r"\bFPS[:=]\s*\d+", re.I),
    }
    total_counts = {k: 0 for k in patterns.keys()}
    file_count = 0

    def _consume_text(txt: str):
        for k, p in patterns.items():
            total_counts[k] += len(p.findall(txt))

    for f in files:
        name = f.name.lower()
        try:
            if name.endswith(".zip"):
                with zipfile.ZipFile(io.BytesIO(f.read())) as zf:
                    for info in zf.infolist():
                        if info.is_dir(): continue
                        if not info.filename.lower().endswith((".txt", ".log")): continue
                        with zf.open(info) as zfh:
                            data = zfh.read()
                            txt = data.decode("utf-8", errors="ignore")
                            _consume_text(txt)
                            file_count += 1
            else:
                data = f.read()
                txt = data.decode("utf-8", errors="ignore")
                _consume_text(txt)
                file_count += 1
        except Exception:
            continue

    parts = [f"{k}:{v}" for k, v in total_counts.items()]
    return {"log_summary": f"files={file_count}; " + ", ".join(parts)}

def _parse_log_summary(summary: str) -> dict:
    out = {"files": 0, "crash": 0, "anr": 0, "gl_err": 0, "thermal": 0, "net": 0, "fps": 0}
    if not summary:
        return out
    try:
        parts = [p.strip() for p in summary.split(";")]
        if parts and parts[0].startswith("files="):
            out["files"] = int(parts[0].split("=",1)[1])
        tail = parts[1] if len(parts) > 1 else ""
        for kv in tail.split(","):
            if ":" in kv:
                k, v = kv.split(":", 1)
                if k.strip() in out:
                    out[k.strip()] = int(v.strip())
    except Exception:
        pass
    return out

def infer_root_causes_from_logs(summary: str) -> list:
    c = _parse_log_summary(summary)
    hyps = []
    if c.get("gl_err", 0) >= 3 or (c.get("fps", 0) >= 10 and c.get("gl_err", 0) >= 1):
        hyps.append({"signal": "gl_err/fps", "hypothesis": "GPU 드라이버/렌더링 병목 가능", "evidence": f"gl_err={c.get('gl_err',0)}, fps={c.get('fps',0)}"})
    if c.get("crash", 0) >= 2:
        hyps.append({"signal": "crash", "hypothesis": "네이티브 크래시(메모리/널포인터) 가능", "evidence": f"crash={c.get('crash',0)}"})
    if c.get("anr", 0) >= 1:
        hyps.append({"signal": "anr", "hypothesis": "메인스레드 블로킹/IO 지연", "evidence": f"anr={c.get('anr',0)}"})
    if c.get("thermal", 0) >= 1:
        hyps.append({"signal": "thermal", "hypothesis": "써멀 스로틀링으로 인한 클럭 저하", "evidence": f"thermal={c.get('thermal',0)}"})
    if c.get("net", 0) >= 2:
        hyps.append({"signal": "net", "hypothesis": "네트워크 지연/SSL 오류", "evidence": f"net={c.get('net',0)}"})
    return hyps

# =========================
# 토큰 절약: 코멘트 압축/태그 추출
# =========================
def compact_text(s: str, max_len=180):
    if not isinstance(s, str): return ""
    s = re.sub(r"\s+", " ", s).strip()
    return s[:max_len]

def digest_comments(series, topn=10, max_len=180):
    vc = (series.astype(str)
          .map(lambda x: re.sub(r"\s+", " ", x or "").strip())
          .replace({"nan": ""})
          .value_counts())
    keys = [compact_text(k, max_len) for k in vc.index.tolist() if k][:topn]
    return keys

def extract_issue_tags(text: str) -> list:
    if not isinstance(text, str): return []
    t = text.lower()
    tags = set()
    # UI/디스플레이/레이아웃
    if re.search(r"(punch[\s\-]?hole|펀치홀)", t): tags.add("punch_hole")
    if re.search(r"(notch|노치)", t): tags.add("notch")
    if re.search(r"(fold|폴더블|플렉스)", t): tags.add("foldable")
    if re.search(r"(rotation|회전|landscape|portrait)", t): tags.add("rotation")
    if re.search(r"(resolution|해상도|dpi|density|텍스처|blur|흐릿|깨짐|아이콘|폰트)", t): tags.add("ui_render")
    # 성능/그래픽/발열
    if re.search(r"(fps|frame|stutter|끊김)", t): tags.add("fps_drop")
    if re.search(r"(thermal|써멀|throttl|발열)", t): tags.add("thermal")
    if re.search(r"(opengl|vulkan|egl|renderer|shader|texture)", t): tags.add("gpu_render")
    # 안정성/네트워크/입력
    if re.search(r"(crash|fatal|예외|크래시)", t): tags.add("crash")
    if re.search(r"(anr|응답없음)", t): tags.add("anr")
    if re.search(r"(ssl|handshake|unknownhost|timeout|네트워크)", t): tags.add("network")
    if re.search(r"(input|터치|반응|딜레이|지연)", t): tags.add("input_delay")
    return sorted(tags)

def normalize_gpu(g):
    s = str(g or "").strip()
    if not s: return ""
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\bPower\s*VR\b", "PowerVR", s, flags=re.I)
    s = re.sub(r"\bIMG\s+GE", "PowerVR GE", s, flags=re.I)
    s = re.sub(r"\bGE(\d+)\b", r"PowerVR GE\1", s, flags=re.I)
    return s

def normalize_chipset(c):
    s = str(c or "").strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\bMTK\b", "MediaTek", s, flags=re.I)
    return s

# =========================
# 군집 다이제스트(스펙축 + 태그축)
# =========================
def make_cluster_digests(df_final: pd.DataFrame,
                         min_group=2,
                         per_cluster_max_samples=8) -> list:
    digests = []

    if "comment_text" not in df_final.columns:
        df_final["comment_text"] = ""

    if "issue_tags" not in df_final.columns:
        df_final["issue_tags"] = df_final["comment_text"].map(extract_issue_tags)

    def _devices(rows, k=6):
        return (rows["Device(Model)"].astype(str)
                .replace("nan","").str.strip()
                .value_counts().head(k).index.tolist())

    # 스펙 축
    for col in ["GPU", "Chipset", "OS"]:
        if col not in df_final.columns: continue
        grp = df_final.groupby(df_final[col].astype(str).str.strip())
        for key, rows in grp:
            keyn = (key or "").strip()
            if not keyn or keyn.lower() in ["nan", "(미기재)"]: continue
            if len(rows) < min_group: continue
            d = {
                "axis": col,
                "value": keyn,
                "size": int(len(rows)),
                "devices": _devices(rows),
                "evidence_comments": digest_comments(rows["comment_text"], topn=per_cluster_max_samples),
                "example_rows": rows.head(3)[["Device(Model)","comment_text"]].to_dict(orient="records")
            }
            digests.append(d)

    # 태그 축
    bucket = defaultdict(list)
    for i, tags in enumerate(df_final["issue_tags"]):
        for t in (tags or []):
            bucket[t].append(i)
    for t, idxs in bucket.items():
        if len(idxs) < min_group: continue
        rows = df_final.iloc[idxs]
        d = {
            "axis": "issue_tag",
            "value": t,
            "size": int(len(rows)),
            "devices": _devices(rows),
            "evidence_comments": digest_comments(rows["comment_text"], topn=per_cluster_max_samples),
            "example_rows": rows.head(3)[["Device(Model)","comment_text"]].to_dict(orient="records")
        }
        digests.append(d)

    digests.sort(key=lambda x: x["size"], reverse=True)
    return digests

# =========================
# GPT: 군집 단위 미니 프롬프트
# =========================
def call_openai_cluster(client, payload: dict, max_retries=4):
    system = (
        "당신은 모바일/게임 QA 수석입니다. 데이터 기반으로 간결하게 작성하세요. "
        "사실 기반 조사형 표현(확인/분석/추정됩니다)을 사용하고, "
        "반드시 JSON 객체 1개만 출력하세요."
    )
    user = (
        "아래 군집에 대해 '현상/발생기기/영향/원인추정/권고'를 작성해 JSON으로만 출력하세요.\n"
        "스키마: {"
        "\"title\": str, "
        "\"symptom\": str, "
        "\"evidence\": [str], "
        "\"impact\": str, "
        "\"cause\": str, "
        "\"recommendation\": str, "
        "\"priority\": \"P0|P1|P2\"}\n\n"
        + pd.io.json.dumps(payload, force_ascii=False)
    )
    last_err = None
    for i in range(max_retries):
        try:
            resp = client.chat.completions.create(
                model="gpt-4o",
                temperature=0.1,
                top_p=0.9,
                max_tokens=500,
                response_format={"type": "json_object"},
                messages=[{"role":"system","content":system},
                          {"role":"user","content":user}],
            )
            txt = resp.choices[0].message.content.strip()
            try:
                return pd.io.json.loads(txt)
            except Exception:
                first, last = txt.find("{"), txt.rfind("}")
                return pd.io.json.loads(txt[first:last+1])
        except Exception as e:
            last_err = e
            time.sleep(min(2**i + i*0.5, 12))
    raise last_err

def write_issue_with_gpt(client, digest: dict, log_hypotheses: list = None):
    payload = {
        "cluster": digest,
        "log_hypotheses": log_hypotheses or []
    }
    return call_openai_cluster(client, payload)

# =========================
# Summary 텍스트 생성(현상/기기/영향/원인/권고)
# =========================
def build_summary_block(issues: list, topn: int = 100) -> str:
    lines = []
    for i, iss in enumerate(issues or [], start=1):
        if i > topn: break
        title  = iss.get("title","이슈")
        symp   = iss.get("symptom","")
        impact = iss.get("impact","")
        cause  = iss.get("cause","(추정 근거 부족)")
        rec    = iss.get("recommendation","")
        dev_line = ""
        evs = iss.get("evidence") or []
        # evidence 안에 모델명이 들어있는 경우 우선 노출
        for e in evs:
            if any(k in str(e) for k in ["Galaxy","Xiaomi","iPhone","OPPO","VIVO","Redmi","Pixel","SM-"]):
                dev_line = str(e); break
        if not dev_line and evs:
            dev_line = str(evs[0])
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

# =========================
# 리포트 작성
# =========================
def write_excel_report(result: dict, df_final: pd.DataFrame, path: str) -> None:
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
        # Executive_Summary (A/C/E)
        exec_rows = [{
            "A. 한 줄 총평": result.get("summary",""),
            "C. 디바이스 리스크": " / ".join([d.get("device_model_or_combo","") for d in (result.get("device_risks") or [])][:5]),
            "E. 릴리스 권고": f"{result.get('release_decision','')} / 조건: {result.get('conditions','')}"
        }]
        pd.DataFrame(exec_rows).to_excel(wr, sheet_name="Executive_Summary", index=False)

        # Summary — 상세 블록
        summary_text = build_summary_block(result.get("issues", []), topn=100)
        if not summary_text:
            summary_text = (
                f"릴리스 권고: {result.get('release_decision','')} / 조건: {result.get('conditions','')}\n"
                f"- 주요 패턴/군집/핵심 문제/우선순위/종합 의견은 Issues 및 Device_Risks를 참조하십시오."
            )
        pd.DataFrame([{"Summary & Insight": summary_text}]).to_excel(wr, sheet_name="Summary", index=False)

        # Issues — 제한 없음
        issues = pd.DataFrame(result.get("issues", []))
        if issues.empty:
            pd.DataFrame([{"title":"(없음)"}]).to_excel(wr, sheet_name="Issues", index=False)
        else:
            issues.to_excel(wr, sheet_name="Issues", index=False)

        # Device_Risks
        risks = pd.DataFrame(result.get("device_risks", []))
        risks.to_excel(wr, sheet_name="Device_Risks", index=False)

        # Evidence_Sample(원본 일부)
        cols = [c for c in ["Sheet","Device(Model)","GPU","Chipset","RAM","OS","Rank","Checklist","comment_text"] if c in df_final.columns]
        if cols:
            disp = df_final[cols].head(200).copy()
            for c in disp.columns:
                if str(disp[c].dtype) == "object" or str(disp[c].dtype).startswith("category"):
                    disp[c] = disp[c].astype(str)
            disp.to_excel(wr, sheet_name="Evidence_Sample", index=False)
        else:
            pd.DataFrame().to_excel(wr, sheet_name="Evidence_Sample", index=False)

        # Cluster_* (선택)
        metrics_in_result = result.get("metrics", {})
        clusters = metrics_in_result.get("clusters", {}) if isinstance(metrics_in_result, dict) else {}
        if isinstance(clusters, dict) and clusters:
            for key, rows in clusters.items():
                try:
                    pd.DataFrame(rows).to_excel(wr, sheet_name=f"Cluster_{key}", index=False)
                except Exception:
                    pass

# =========================
# UI: 파일 업로드 + 실행
# =========================
uploaded_file = st.file_uploader("원본 QA 엑셀 파일을 업로드하세요", type=["xlsx"])
# 로그 입력은 비활성(필요 시 주석 해제)
log_files = None  # st.file_uploader("Logcat 파일 (.txt/.log/.zip, 다중)", type=["txt","log","zip"], accept_multiple_files=True)
st.caption("※ Logcat 분석은 현재 비활성화 상태입니다.")

if uploaded_file:
    with step_status("엑셀 로드"):
        xls = pd.ExcelFile(uploaded_file, engine="openpyxl")
        diag_dump("시트 목록", xls.sheet_names)

    with step_status("테스트 시트 자동감지"):
        test_candidates = find_test_sheet_candidates(xls)
        if not test_candidates:
            test_candidates = xls.sheet_names
        diag_dump("감지된 후보 시트", test_candidates)

    st.subheader("1. 테스트 시트 선택")
    test_sheets_selected = st.multiselect(
        "자동 감지된 테스트 시트 중 분석 대상 선택",
        options=test_candidates,
        default=test_candidates[:2]
    )
    if not test_sheets_selected:
        st.error("❌ 최소 1개 이상 선택해야 합니다.")
        st.stop()

    st.subheader("2. 스펙 시트 선택 (디바이스 리스트)")
    default_spec = [s for s in ["AOS_Device_List", "iOS_Device_List"] if s in xls.sheet_names]
    spec_sheets_selected = st.multiselect(
        "스펙(GPU/Chipset/OS/Rank 등) 포함 시트 선택",
        options=xls.sheet_names,
        default=default_spec
    )
    st.markdown("---")

    if st.button("분석 및 리포트 생성", type="primary"):
        # 상태 초기화
        log_summary = {}
        log_hypotheses = []
        clusters_meta = {}

        # 3) Fail + 셀 코멘트 추출
        with step_status("Fail + 셀 코멘트 추출"):
            wb = openpyxl.load_workbook(uploaded_file, data_only=True)
            df_issue = extract_comments_as_dataframe(wb, test_sheets_selected)
            if df_issue is None or df_issue.empty:
                st.error("❌ Fail + 코멘트가 포함된 항목을 찾지 못했습니다.")
                st.stop()

        # 4) 비고/Notes 병합
        with step_status("비고/Notes 병합"):
            df_issue = enrich_with_column_comments(xls, test_sheets_selected[0], df_issue)
            diag_dump("병합 결과 샘플", df_issue.head(10))

        # 5) 스펙 병합
        with step_status("스펙 병합"):
            df_final = df_issue.copy()
            match_rate = 0.0
            if spec_sheets_selected:
                try:
                    spec_frames = [load_std_spec_df(xls, s) for s in spec_sheets_selected]
                    df_spec_all = pd.concat(spec_frames, ignore_index=True)
                    df_spec_all = df_spec_all.drop_duplicates(subset=["model_norm"], keep="first")

                    df_final["model_norm"] = df_final["Device(Model)"].apply(normalize_model_name_strict)
                    df_final = pd.merge(df_final, df_spec_all, on="model_norm", how="left")

                    for col in ["GPU", "제조사", "Chipset", "RAM", "OS", "Rank", "Model"]:
                        cx, cy = f"{col}_x", f"{col}_y"
                        if cx in df_final.columns and cy in df_final.columns:
                            df_final[col] = df_final[cx].where(df_final[cx].notna(), df_final[cy])
                            df_final.drop(columns=[cx, cy], inplace=True)
                        elif cx in df_final.columns:
                            df_final.rename(columns={cx: col}, inplace=True)
                        elif cy in df_final.columns:
                            df_final.rename(columns={cy: col}, inplace=True)
                    if "GPU" in df_final.columns:
                        df_final["GPU"] = df_final["GPU"].apply(normalize_gpu)
                    if "Chipset" in df_final.columns:
                        df_final["Chipset"] = df_final["Chipset"].apply(normalize_chipset)

                    if "GPU" in df_final.columns:
                        matched = int(df_final["GPU"].notna().sum())
                        match_rate = round(matched / len(df_final) * 100, 1)
                        st.success(f"스펙 매칭 결과: {matched} / {len(df_final)} 건 ({match_rate}%)")
                except Exception as e:
                    st.warning(f"스펙 병합 중 일부 오류: {e}")

        # 6) Logcat (옵션)
        with step_status("Logcat 분석"):
            if log_files:
                log_summary = load_and_summarize_logcat_files(log_files)
                st.info(f"Logcat 요약: {log_summary.get('log_summary','-')}")
                log_hypotheses = infer_root_causes_from_logs(log_summary.get("log_summary", ""))
                diag_dump("로그 근본 원인 가설", log_hypotheses)
            else:
                st.info("로그 파일 없음. Logcat 분석 생략.")

        # 7) 군집 다이제스트
        with step_status("군집 다이제스트 생성"):
            cluster_digests = make_cluster_digests(df_final, min_group=2, per_cluster_max_samples=8)
            diag_dump("군집 다이제스트", cluster_digests[:10])
            # 군집 메타 기록(엑셀 Cluster_* 시트 용)
            clusters_meta = {
                "spec": [{"axis": d["axis"], "value": d["value"], "size": d["size"]}
                         for d in cluster_digests if d["axis"] in ["GPU","Chipset","OS"]],
                "tags": [{"axis": d["axis"], "value": d["value"], "size": d["size"]}
                         for d in cluster_digests if d["axis"] == "issue_tag"],
            }

        # 8) 군집 단위 GPT 코멘트 생성
        with step_status("GPT 코멘트 생성(군집 단위)"):
            issues = []
            for dig in cluster_digests:
                try:
                    draft = write_issue_with_gpt(client, dig, log_hypotheses=log_hypotheses)
                except Exception as e:
                    # 실패 시 최소한의 규칙 기반 백업
                    draft = {
                        "title": f"{dig['axis']}:{dig['value']} 군집 이슈",
                        "symptom": "공통 현상 발생",
                        "evidence": dig.get("evidence_comments", [])[:3],
                        "impact": "사용자 경험 저하",
                        "cause": "원인 추정 필요(로그/리프로 보강)",
                        "recommendation": "재현 로그 확보 및 조건화된 리프로 적용",
                        "priority": "P1"
                    }
                issues.append({
                    "title": draft.get("title", f"{dig['axis']}:{dig['value']} 군집 이슈"),
                    "symptom": draft.get("symptom",""),
                    "reproduction": "군집 내 공통 조건에서 반복 재현됨",
                    "evidence": draft.get("evidence", dig.get("evidence_comments", [])[:3]),
                    "impact": draft.get("impact","사용자 경험 저하"),
                    "priority": draft.get("priority","P1"),
                    "cause": draft.get("cause","원인 추정 필요"),
                    "recommendation": draft.get("recommendation","재현 로그 확보 및 리프로 적용"),
                })

            # 미군집(단일·흩어진 것들)도 누락 없이 추가
            clustered_idx = set()
            # 간단화를 위해 example_rows의 index를 쓰지 않고, cluster 커버에서 제외된 추정 개수는 생략
            # 필요하면 df_final.index를 추적하는 로직을 추가하세요.

        # 9) 결과 조립 및 저장
        with step_status("리포트 저장"):
            result = {
                "summary": "군집 단위로 생성된 데이터 기반 코멘트입니다.",
                "issues": issues,
                "device_risks": [
                    {"device_model_or_combo": ", ".join(d["devices"][:6]) or "(다수)",
                     "reason": f"{d['axis']}:{d['value']} 군집에서 재현 빈도 높음",
                     "impact": "대상 군집 사용자 체감 영향 큼"}
                    for d in cluster_digests if d["size"] >= 3
                ],
                "actions": [],
                "release_decision": "Conditional",
                "conditions": "상위 군집 패치 적용 및 재테스트 통과 시 배포",
                "metrics": {"clusters": clusters_meta}
            }
            output = "QA_Report.xlsx"
            write_excel_report(result, df_final, output)
            st.success("✅ 리포트 생성 완료")
            with open(output, "rb") as f:
                st.download_button("📊 Excel 리포트 다운로드", f.read(), file_name=output)

        # 표시용 샘플
        st.success(f"{len(df_final)}개의 'Fail' 항목 분석 완료.")
        st.dataframe(df_final.head(20), use_container_width=True)
