# -*- coding: utf-8 -*-
# 최종 Streamlit 앱: QA 결과 자동 코멘트 생성기 (헤더자동탐지/동의어매핑/모델정규화 적용)

import os
import re
import io
import zipfile
import unicodedata
import time
from contextlib import contextmanager

import pandas as pd
import openpyxl
import streamlit as st
from dotenv import load_dotenv
from openai import OpenAI

from qa_patch_module import (
    find_test_sheet_candidates,
    enrich_with_column_comments,
    build_system_prompt, build_user_prompt,
    parse_llm_json, write_excel_report,
    self_check
)
# =========================
# 환경설정
# =========================
load_dotenv()
# 우선순위: st.secrets > .env > os.environ
api_key = st.secrets.get("OPENAI_API_KEY", os.getenv("OPENAI_API_KEY", ""))
if not api_key:
    st.error("OpenAI API 키가 없습니다. st.secrets 또는 .env에 OPENAI_API_KEY를 설정하세요.")
    st.stop()

client = OpenAI(api_key=api_key)

st.set_page_config(page_title="QA 결과 자동 코멘트 생성기", layout="wide")
st.title(":bar_chart: QA 결과 자동 코멘트 생성기")
# ▼ 프로젝트/버전 입력 UI (이 줄을 st.title 아래에 추가)
col_pj, col_ver, col_reset = st.columns([2, 2, 1])
with col_pj:
    project_name = st.text_input("프로젝트명", value="", placeholder="예: AOD v1.3 CO")
with col_ver:
    checklist_version = st.text_input("체크리스트 버전", value="", placeholder="예: r1.2.0")
with col_reset:
    if st.button("🔄 세션 초기화"):
        st.session_state.clear()
        st.experimental_rerun()


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

# =========================
# 공통 유틸
# =========================
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
# Log 분석: 요약 + 근본 원인 추정
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
        hyps.append({"signal": "anr", "hypothesis": "메인스레드 블로킹/IO 지연으로 인한 ANR 가능", "evidence": f"anr={c.get('anr',0)}"})
    if c.get("thermal", 0) >= 1:
        hyps.append({"signal": "thermal", "hypothesis": "써멀 스로틀링으로 인한 클럭 저하", "evidence": f"thermal={c.get('thermal',0)}"})
    if c.get("net", 0) >= 2:
        hyps.append({"signal": "net", "hypothesis": "네트워크 지연/SSL 오류로 인한 UX 저하 가능", "evidence": f"net={c.get('net',0)}"})
    return hyps

# =========================
# 나머지 메인 파이프라인
# =========================
# (아래 생략 없이 전체 - 군집 통계, LLM 호출, 보고서 생성 모두 동일)
# =========================
# UI: 파일 업로드 및 실행
# =========================
uploaded_file = st.file_uploader("원본 QA 엑셀 파일을 업로드하세요", type=["xlsx"])
log_files = None #st.file_uploader("Logcat 파일 업로드(.txt/.log 또는 .zip, 다중 가능)", type=["txt", "log", "zip"], accept_multiple_files=True)
st.caption("※ Logcat 분석은 현재 비활성화 상태입니다.")

if uploaded_file:
    with step_status("엑셀 로드"):
        xls = pd.ExcelFile(uploaded_file, engine="openpyxl")
        diag_dump("시트 목록", xls.sheet_names)

    # 1) 테스트 시트 자동감지
    with step_status("테스트 시트 자동감지"):
        test_candidates = find_test_sheet_candidates(xls)
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

    # 2) 스펙 시트 선택
    st.subheader("2. 스펙 시트 선택 (디바이스 리스트)")
    default_spec = [s for s in ["AOS_Device_List", "iOS_Device_List"] if s in xls.sheet_names]
    spec_sheets_selected = st.multiselect(
        "스펙(Chipset, GPU, OS, Rank 등) 포함 시트 선택",
        options=xls.sheet_names,
        default=default_spec
    )
    st.markdown("---")

    if st.button("분석 및 리포트 생성", type="primary"):
        # 🔒 실행별 상태 초기화 (이전 실행 값 섞임 방지)
        log_summary = {}
        log_hypotheses = []
        clusters = {}
        evidence_links = []


        # 3) Fail+코멘트 추출
        with step_status("Fail + 셀 코멘트 추출"):
            wb = openpyxl.load_workbook(uploaded_file, data_only=True)
            df_issue = None
            try:
                from qa_patch_module import enrich_with_column_comments
                df_issue = pd.DataFrame()
                tmp = []
                for s in test_sheets_selected:
                    ws = wb[s]
                    for row in ws.iter_rows():
                        for cell in row:
                            if isinstance(cell.value, str) and cell.value.lower() == "fail" and cell.comment:
                                tmp.append({
                                    "Sheet": s,
                                    "Checklist": ws.title,
                                    "Device(Model)": "",
                                    "comment_cell": cell.comment.text.strip()
                                })
                df_issue = pd.DataFrame(tmp)
            except Exception:
                pass
            if df_issue is None or df_issue.empty:
                st.warning("❌ Fail+코멘트 항목이 없습니다.")
                st.stop()

        # 4) 비고/Notes 병합
        with step_status("비고/Notes 병합"):
            df_issue = enrich_with_column_comments(xls, test_sheets_selected[0], df_issue)
            diag_dump("병합 결과 샘플", df_issue.head(10))

        # 5) 자가진단
        with step_status("모듈 자가진단"):
            diag = self_check(df_issue)
            diag_dump("self_check 결과", diag)
            if not diag["row_ok"]:
                st.error("❌ 유효한 데이터 없음. 중단.")
                st.stop()

        df_final = df_issue.copy()

        # 6) Logcat 요약 + 원인추정
        with step_status("Logcat 분석"):
            log_summary, log_hypotheses = {}, []
            if log_files:
                log_summary = load_and_summarize_logcat_files(log_files)
                st.info(f"Logcat 요약: {log_summary.get('log_summary','-')}")
                log_hypotheses = infer_root_causes_from_logs(log_summary.get("log_summary", ""))
                diag_dump("로그 근본 원인 가설", log_hypotheses)
            else:
                st.info("로그 파일 없음. Logcat 분석 생략.")

        # 7) 군집 통계
        with step_status("군집(Cluster) 통계 산출"):
            def _cluster_counts(df, col, topn=15):
                if col not in df.columns:
                    return pd.DataFrame(columns=[col, "count"])
                vc = df[col].fillna("(미기재)").astype(str).str.strip().value_counts().head(topn)
                return vc.reset_index().rename(columns={"index": col, 0: "count"})
            cluster_gpu = _cluster_counts(df_final, "GPU")
            cluster_chip = _cluster_counts(df_final, "Chipset")
            clusters = {
                "by_gpu": cluster_gpu.to_dict(orient="records"),
                "by_chipset": cluster_chip.to_dict(orient="records"),
            }
            diag_dump("GPU/Chipset 군집 통계", clusters)

        # 8) 프롬프트 준비
        metrics = {
            "total_fail_issues": len(df_final),
            "clusters": clusters,
            "log_hypotheses": log_hypotheses
        }
        deltas, evidence_links = {}, []
        if log_summary:
            evidence_links.append(f"Log Summary: {log_summary.get('log_summary','')}")

        base_kwargs = {
            "project": (project_name.strip() or "UNKNOWN_PROJECT"),
            "version": (checklist_version.strip() or "UNKNOWN_VERSION"),
            "metrics": metrics,
            "deltas": deltas,
            "evidence_links": evidence_links,
            "sample_issues": df_final,
            "max_rows": 500  # 필요 시 늘림
        }

        # 9) 토큰 예산 자동 조정
        def _rough_token_count(t: str) -> int:
            return max(1, int(len(t) / 2.5))
        def estimate_tokens(msgs: list) -> int:
            try:
                import tiktoken
                enc = tiktoken.get_encoding("cl100k_base")
                return sum(len(enc.encode(m.get("content",""))) for m in msgs)
            except Exception:
                return sum(_rough_token_count(m.get("content","")) for m in msgs)
        def fit_prompt(build_user, base_kwargs, model_budget=120000, reserve_output=6000):
            max_rows_list = [800, 600, 400, 300, 200, 100]
            df = base_kwargs["sample_issues"]
            for mr in max_rows_list:
                kwargs = dict(base_kwargs)
                kwargs["sample_issues"] = df.head(mr)
                sp = build_system_prompt()
                up = build_user(**kwargs)
                used = estimate_tokens([{"content": sp},{"content": up}])
                if used + reserve_output < model_budget:
                    return sp, up, {"prompt_tokens_est": used, "max_rows": mr}
            return sp, up, {"warn": "budget_exceeded"}

        with step_status("토큰 예산 조정"):
            sp, up, diag_budget = fit_prompt(build_user_prompt, base_kwargs)
            diag_dump("토큰 진단", diag_budget)

        # 10) OpenAI 호출
        with st.spinner("GPT가 리포트를 작성 중입니다..."):
            try:
                resp = client.chat.completions.create(
                    model="gpt-4o",
                    temperature=0.1,
                    top_p=0.9,
                    messages=[{"role":"system","content":sp},{"role":"user","content":up}],
                )
                raw = resp.choices[0].message.content
                result = parse_llm_json(raw)
                result["metrics"] = metrics  # 군집·로그 근거 보존
                diag_dump("LLM 원문(요약)", raw[:4000])
            except Exception as e:
                st.error(f"OpenAI 호출 오류: {e}")
                st.stop()

        # 11) 엑셀 리포트 생성
        try:
            output = "QA_Report.xlsx"
            write_excel_report(result, df_final, output)
            st.success("✅ 리포트 생성 완료")
            with open(output, "rb") as f:
                st.download_button("📊 Excel 리포트 다운로드", f.read(), file_name=output)
        except Exception as e:
            st.error(f"리포트 생성 오류: {e}")

