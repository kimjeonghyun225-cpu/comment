# -*- coding: utf-8 -*-
# comment1.py — 프로젝트명 입력 없이, 세션 초기화 버튼으로 런→리셋→재분석 플로우
# Fail 리스트 → 스펙 병합 → 코멘트/스펙 정규화 → 군집화 → GPT 요약 → Excel 리포트

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
    self_check,
    load_std_spec_df,                 # ✅ 스펙 병합 유틸
    extract_comments_as_dataframe     # ✅ Fail+코멘트(메타 포함) 추출
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

# 세션 초기화 버튼만 사용 (프로젝트명 입력 UI 없음)
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
# Log 분석: 요약 + 근본 원인 추정 (현재 업로드 비활성)
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
# UI: 파일 업로드
# =========================
uploaded_file = st.file_uploader("원본 QA 엑셀 파일을 업로드하세요", type=["xlsx"])
log_files = None   # 필요 시 활성화
st.caption("※ Logcat 분석은 현재 비활성화 상태입니다. (세션 초기화 후 새 파일로 재분석하세요)")

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
        # 실행별 상태변수 초기화 (다른 프로젝트 결과 섞임 방지)
        log_summary = {}
        log_hypotheses = []
        clusters = {}
        evidence_links = []

        # 3) Fail + 셀 코멘트 추출
        with step_status("Fail + 셀 코멘트 추출"):
            wb = openpyxl.load_workbook(uploaded_file, data_only=True)
            df_issue = extract_comments_as_dataframe(wb, test_sheets_selected)
            diag_dump("Fail 추출 샘플(최대 10)", df_issue.head(10) if df_issue is not None else None)

        if df_issue is None or df_issue.empty:
            st.warning("❌ Fail+코멘트 항목을 찾지 못했습니다.")
            st.stop()

        # 4) 비고/Notes 병합
        with step_status("비고/Notes 병합"):
            df_issue = enrich_with_column_comments(xls, test_sheets_selected[0], df_issue)
            diag_dump("병합 결과 샘플", df_issue.head(10))

        # 5) 스펙 시트 병합
        df_final = df_issue.copy()
        match_rate = 0.0
        if spec_sheets_selected:
            with step_status("스펙 병합"):
                try:
                    spec_frames = [load_std_spec_df(xls, s) for s in spec_sheets_selected]
                    df_spec_all = pd.concat(spec_frames, ignore_index=True).drop_duplicates(subset=["model_norm"], keep="first")

                    df_final["model_norm"] = df_final["Device(Model)"].apply(normalize_model_name_strict)
                    df_final = pd.merge(df_final, df_spec_all, on="model_norm", how="left")

                    for col in ["GPU","제조사","Chipset","RAM","OS","Rank","Model"]:
                        cx, cy = f"{col}_x", f"{col}_y"
                        if cx in df_final.columns and cy in df_final.columns:
                            df_final[col] = df_final[cx].where(df_final[cx].notna(), df_final[cy])
                            df_final.drop(columns=[cx, cy], inplace=True)
                        elif cx in df_final.columns:
                            df_final.rename(columns={cx: col}, inplace=True)
                        elif cy in df_final.columns:
                            df_final.rename(columns={cy: col}, inplace=True)

                    if "GPU" in df_final.columns:
                        matched = int(df_final["GPU"].notna().sum())
                        match_rate = round(matched / len(df_final) * 100, 1)
                        st.success(f"스펙 매칭 결과: {matched} / {len(df_final)} 건 ({match_rate}%)")
                except Exception as e:
                    st.error(f"스펙 병합 중 오류: {e}")
        else:
            df_final = df_issue.copy()

        # 6) 모듈 자가진단
        with step_status("모듈 자가진단"):
            diag = self_check(df_final)
            diag_dump("self_check 결과", diag)
            if not diag["row_ok"]:
                st.error("❌ 유효한 데이터 없음. 중단.")
                st.stop()

        # 7) 코멘트 정리 + GPU 정규화/계열 분류 + Chipset 기반 보강
        def clean_comment_text(s: str) -> str:
            if pd.isna(s): return ""
            t = str(s)
            t = re.sub(r"https?://go\.microsoft\.com/.*", " ", t)
            t = re.sub(r"Excel에서 이 스레드 댓글을.*?자세한 정보.*?:", " ", t)
            t = re.sub(r"\s+", " ", t).strip(" -:|,.;\n\t")
            for pat, rep in [
                (r"프레임\s*드랍|프레임드랍|프레임\s*저하|프레임\s*하락", "프레임 드랍"),
                (r"렉|랙|버벅|버벅임|끊김|지연", "입력 지연"),
                (r"발열|과열", "발열"),
                (r"크래시|강제종료|튕김", "크래시"),
                (r"텍스처\s*깨짐|그래픽\s*깨짐|렌더링\s*오류", "그래픽 깨짐"),
                (r"화면\s*회전\s*불가|회전\s*안됨", "화면 회전 문제"),
                (r"ANR|응답없음", "ANR"),
            ]:
                t = re.sub(pat, rep, t, flags=re.I)
            return t

        df_final["comment_text_norm"] = df_final.get("comment_text", "").astype(str).apply(clean_comment_text)

        def normalize_gpu_name(s: str) -> str:
            if pd.isna(s) or not str(s).strip(): return ""
            x = str(s).strip().replace("–","-").replace("—","-").replace("_"," ")
            x = re.sub(r"\s+"," ", x)
            x = re.sub(r"\bPower\s*VR\b", "PowerVR", x, flags=re.I)
            x = re.sub(r"\bIMG\s+GE", "PowerVR GE", x, flags=re.I)
            x = re.sub(r"\bIMG\s+GT", "PowerVR GT", x, flags=re.I)
            x = re.sub(r"\bGE(\d+)\b", r"PowerVR GE\1", x, flags=re.I)
            x = re.sub(r"\bGT(\d+)\b", r"PowerVR GT\1", x, flags=re.I)
            x = re.sub(r"\bAdreno\s*-?\s*(\d+)", r"Adreno \1", x, flags=re.I)
            x = re.sub(r"\bMali[\s\-]*G\s*(\d+)\s*MP?\s*(\d+)\b", r"Mali-G\1 MP\2", x, flags=re.I)
            x = re.sub(r"\bMali[\s\-]*G\s*(\d+)\b", r"Mali-G\1", x, flags=re.I)
            x = re.sub(r"\bMali[\s\-]*T\s*(\d+)\s*MP?\s*(\d+)\b", r"Mali-T\1 MP\2", x, flags=re.I)
            x = re.sub(r"\bMali[\s\-]*T\s*(\d+)\b", r"Mali-T\1", x, flags=re.I)
            x = re.sub(r"\bApple\s*(GPU)?\s*\(?(\d+)\s*[- ]?core\)?", r"Apple GPU \2-core", x, flags=re.I)
            x = re.sub(r"\bVivante\s*(GC|GT)\s*(\d+)", r"Vivante \1\2", x, flags=re.I)
            x = re.sub(r"\bTegra\s*(K1|X1|X2)\b", r"Tegra \1", x, flags=re.I)
            return x

        def classify_gpu_family(x: str) -> str:
            y = (x or "").lower()
            if "adreno" in y: return "Adreno"
            if "mali" in y: return "Mali"
            if "powervr" in y or "img ge" in y or "img gt" in y: return "PowerVR"
            if "apple gpu" in y: return "Apple"
            if "vivante" in y: return "Vivante"
            if "tegra" in y or "nvidia" in y: return "Tegra"
            return "Other" if y else ""

        def infer_gpu_from_chipset(s: str) -> str:
            t = ("" if pd.isna(s) else str(s)).lower()
            if not t: return ""
            if "snapdragon" in t or "qualcomm" in t: return "Adreno (inferred)"
            if "mediatek" in t or "dimensity" in t or "helio" in t: return "Mali (inferred)"
            if "exynos" in t: return "Mali (inferred)"
            if "kirin" in t or "hisilicon" in t: return "Mali (inferred)"
            if re.search(r"\bapple\s*a\d+\b", t): return "Apple GPU (inferred)"
            if "unisoc" in t or "spreadtrum" in t: return "Mali (inferred)"
            return ""

        df_final["GPU"] = df_final.get("GPU","").astype(str).apply(normalize_gpu_name)
        miss = df_final["GPU"].eq("") | df_final["GPU"].isna()
        if "Chipset" in df_final.columns and miss.any():
            df_final.loc[miss, "GPU"] = df_final.loc[miss, "Chipset"].apply(infer_gpu_from_chipset)
        df_final["GPU_Family"] = df_final["GPU"].apply(classify_gpu_family)

        # 8) 군집(Cluster) 통계 + 상세(계열×증상) 생성
        with step_status("군집(Cluster) 통계 산출"):
            def _cluster_counts(df, col, topn=20):
                if col not in df.columns:
                    st.info(f"군집 스킵: '{col}' 없음"); return pd.DataFrame(columns=[col,"count"])
                nn = int(df[col].replace("", pd.NA).notna().sum())
                if nn == 0:
                    st.info(f"군집 스킵: '{col}' 모두 결측"); return pd.DataFrame(columns=[col,"count"])
                vc = (df[col].fillna("(미기재)").astype(str).str.strip()
                      .replace("", "(미기재)").value_counts().head(topn))
                return vc.reset_index().rename(columns={"index": col, 0: "count"})

            cluster_gpu_family = _cluster_counts(df_final, "GPU_Family")
            cluster_gpu_model  = _cluster_counts(df_final, "GPU")
            cluster_chip       = _cluster_counts(df_final, "Chipset")

            def build_signature(s: str) -> str:
                t = ("" if pd.isna(s) else str(s)).lower()
                keep = []
                for kw in ["프레임 드랍","그래픽 깨짐","입력 지연","크래시","발열","화면 회전 문제","anr","네트워크","사운드","로딩 지연","메모리"]:
                    if kw in t: keep.append(kw)
                return " | ".join(sorted(set(keep))) or t[:40]

            df_final["issue_signature"] = df_final["comment_text_norm"].apply(build_signature)

            def top_models(s, n=3):
                return [str(x) for x in pd.Series(s).dropna().astype(str).head(n)]

            grp = (df_final
                   .groupby(["GPU_Family","issue_signature"], dropna=False)
                   .agg(count=("issue_signature","size"),
                        repr_models=("Device(Model)", lambda s: top_models(s, 3)),
                        evidence_rows=("comment_text_norm", lambda s: [str(x) for x in pd.Series(s).dropna().astype(str).head(3)]))
                   .reset_index()
                   .sort_values("count", ascending=False))

            clusters = {
                "by_gpu_family": cluster_gpu_family.to_dict(orient="records"),
                "by_gpu":        cluster_gpu_model.to_dict(orient="records"),
                "by_chipset":    cluster_chip.to_dict(orient="records"),
                "detailed": [
                    {
                        "dimension": "GPU_Family",
                        "value": r["GPU_Family"] or "",
                        "signature": r["issue_signature"] or "",
                        "count": int(r["count"]),
                        "repr_models": r["repr_models"],
                        "evidence_rows": r["evidence_rows"]
                    }
                    for _, r in grp.iterrows() if r["count"] >= 2
                ]
            }
            diag_dump("군집 통계/상세", clusters)

        # 9) 프롬프트 준비 (프로젝트/버전은 빈값으로 전달)
        metrics = {
            "total_fail_issues": len(df_final),
            "by_gpu_family": clusters["by_gpu_family"],
            "by_gpu": clusters["by_gpu"],
            "by_chipset": clusters["by_chipset"],
            "clusters_detailed": clusters["detailed"],
            "log_hypotheses": []  # log_files 비활성 상태
        }
        deltas, evidence_links = {}, []

        base_kwargs = {
            "project": "",                 # ✅ 프로젝트명 미사용
            "version": "",                 # ✅ 버전 미사용
            "metrics": metrics,
            "deltas": deltas,
            "evidence_links": evidence_links,
            "sample_issues": df_final,
            "max_rows": 500
        }

        # 10) 토큰 예산 자동 조정
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

        # 11) OpenAI 호출
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
                result["metrics"] = metrics
                diag_dump("LLM 원문(요약)", raw[:4000])
            except Exception as e:
                st.error(f"OpenAI 호출 오류: {e}")
                st.stop()

        # 12) 엑셀 리포트 생성
        try:
            output = "QA_Report.xlsx"
            write_excel_report(result, df_final, output)
            st.success("✅ 리포트 생성 완료")
            with open(output, "rb") as f:
                st.download_button("📊 Excel 리포트 다운로드", f.read(), file_name=output)
        except Exception as e:
            st.error(f"리포트 생성 오류: {e}")
