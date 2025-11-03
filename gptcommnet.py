# -*- coding: utf-8 -*-

# 최종 Streamlit 앱: QA 결과 자동 코멘트 생성기
# - 세션 초기화 버튼 제공(프로젝트명 입력 없이 사용)
# - Fail + 코멘트 추출(행 기반 디바이스/OS 동시 추출) → 비고 병합(모든 선택 시트) → 스펙 병합
# - GPU/CPU(Chipset) 군집 + Feature 군집
# - 토큰 절감(대표 샘플 압축) → gpt-4o(JSON 강제) → Excel 리포트

import os
import re
import io
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
api_key = st.secrets.get("OPENAI_API_KEY", os.getenv("OPENAI_API_KEY", ""))
if not api_key:
    st.error("OpenAI API 키가 없습니다. st.secrets 또는 .env에 OPENAI_API_KEY를 설정하세요.")
    st.stop()
client = OpenAI(api_key=api_key)

st.set_page_config(page_title="QA 결과 자동 코멘트 생성기", layout="wide")
st.title(":bar_chart: QA 결과 자동 코멘트 생성기")

# 세션 초기화 버튼(프로젝트 간 혼입 방지)
col_reset = st.columns([1])[0]
with col_reset:
    if st.button("🔄 세션 초기화"):
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
# 헤더 탐지: 행 기반 디바이스/OS 추출용
# =========================
def _detect_header_map(ws, max_scan_rows=40):
    """
    워크시트 상단에서 헤더 행을 찾고, 디바이스/OS 관련 칼럼 인덱스를 매핑.
    반환: {"header_row": r, "device_col": c or None, "os_col": c or None}
    """
    def _norm_cell(v):
        s = unicodedata.normalize("NFKC", str(v or "")).lower().strip()
        s = re.sub(r"[\s\-\_/()\[\]{}:+·∙•]", "", s)  # 공백/구분자 제거
        return s

    # 폭넓은 패턴(정확일치 + 부분일치)
    dev_exact = re.compile(r"^(device|model|모델|모델명|제품|제품명|단말|단말기명)$")
    dev_contains = re.compile(r"(device|model|모델|모델명|제품|제품명|단말)")
    os_exact  = re.compile(r"^(os|osversion|android|ios|펌웨어|소프트웨어버전)$")
    os_contains = re.compile(r"(os|android|ios|펌웨어|소프트웨어)")

    header_row, device_col, os_col = None, None, None
    max_c = ws.max_column

    for r in range(1, min(max_scan_rows, ws.max_row) + 1):
        values = [ws.cell(row=r, column=c).value for c in range(1, max_c + 1)]
        normed = [_norm_cell(v) for v in values]
        if sum(1 for v in normed if v) < 2:
            continue

        d_idx, o_idx = None, None
        for ci, v in enumerate(normed, start=1):
            if not v:
                continue
            # 디바이스 컬럼 후보
            if dev_exact.match(v) or dev_contains.search(v) or v in ("devicemodel", "devicemodelname", "devicename"):
                if d_idx is None:
                    d_idx = ci
            # OS 컬럼 후보
            if os_exact.match(v) or os_contains.search(v):
                if o_idx is None:
                    o_idx = ci

        if d_idx or o_idx:
            header_row, device_col, os_col = r, d_idx, o_idx
            break

    return {"header_row": header_row, "device_col": device_col, "os_col": os_col}

# =========================
# 파일 업로드
# =========================
st.caption("※ Logcat 분석은 현재 비활성화 상태입니다.")
uploaded_file = st.file_uploader("원본 QA 엑셀 파일을 업로드하세요", type=["xlsx"])

if uploaded_file:
    # 파일 포인터 고정
    data = uploaded_file.read()

    with step_status("엑셀 로드"):
        xls = pd.ExcelFile(io.BytesIO(data), engine="openpyxl")
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

    # 실행
    if st.button("분석 및 리포트 생성", type="primary"):
        log_hypotheses = []
        clusters = {}
        evidence_links = []

        # 3) Fail + 셀 코멘트 추출 (행 기반 디바이스/OS 동시 추출)
        with step_status("Fail + 셀 코멘트 추출"):
            wb = openpyxl.load_workbook(io.BytesIO(data), data_only=True)
            df_issue = []

            for s in test_sheets_selected:
                ws = wb[s]
                hdr = _detect_header_map(ws, max_scan_rows=12)
                header_row = hdr.get("header_row")
                dev_col = hdr.get("device_col")
                os_col  = hdr.get("os_col")

                for row in ws.iter_rows():
                    for cell in row:
                        if isinstance(cell.value, str) and cell.value.strip().lower() == "fail" and cell.comment:
                            dev_val = ""
                            os_val = ""
                            if header_row and cell.row > header_row:
                                if dev_col:
                                    dev_val = ws.cell(row=cell.row, column=dev_col).value
                                if os_col:
                                    os_val = ws.cell(row=cell.row, column=os_col).value

                            df_issue.append({
                                "Sheet": s,
                                "Checklist": ws.title,
                                "Device(Model)": str(dev_val or "").strip(),
                                "OS": str(os_val or "").strip(),
                                "comment_cell": (cell.comment.text or "").strip()
                            })

            df_issue = pd.DataFrame(df_issue) if df_issue else pd.DataFrame(
                columns=["Sheet", "Checklist", "Device(Model)", "OS", "comment_cell"]
            )
            if df_issue.empty:
                st.warning("❌ Fail+코멘트 항목이 없습니다(셀 코멘트 기준). 비고/Notes만으로도 군집화하려면 원본 시트의 비고열을 활용하십시오.")
                st.stop()

        # 4) 비고/Notes 병합 (모든 선택 시트에 대해 병합)
        with step_status("비고/Notes 병합"):
            for _sheet in test_sheets_selected:
                df_issue = enrich_with_column_comments(xls, _sheet, df_issue)
            diag_dump("병합 결과 샘플", df_issue.head(10))

        # 5) 스펙 병합
        df_final = df_issue.copy()
        match_rate = 0.0
        if spec_sheets_selected:
            with step_status("스펙 병합"):
                try:
                    def _norm_for_header(s: str) -> str:
                        s = unicodedata.normalize("NFKC", str(s))
                        s = re.sub(r"[\s\-\_/()\[\]{}:+·∙•]", "", s)
                        return s.lower().strip()

                    def find_header_row_for_spec(xls, sheet, max_scan_rows=12):
                        df_probe = pd.read_excel(xls, sheet_name=sheet, header=None, engine="openpyxl")
                        header_row_idx = 0
                        header_candidates = [r"^model$", r"^device$", r"^제품명$", r"^제품$", r"^모델명$", r"^모델$"]
                        for r in range(min(max_scan_rows, len(df_probe))):
                            row_vals = df_probe.iloc[r].astype(str).fillna("")
                            norm_vals = [_norm_for_header(v) for v in row_vals]
                            for v in norm_vals:
                                if any(re.search(pat, v) for pat in header_candidates):
                                    header_row_idx = r; break
                            if header_row_idx: break
                        return header_row_idx

                    def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
                        original_cols = list(df.columns)
                        norm_cols = [_norm_for_header(c) for c in original_cols]
                        col_map = {}
                        synonyms = {
                            r"^(model|device|제품명|제품|모델명|모델)$": "Model",
                            r"^(maker|manufacturer|brand|oem|제조사|벤더)$": "제조사",
                            r"^(gpu|그래픽|그래픽칩|그래픽스|그래픽프로세서)$": "GPU",
                            r"^(chipset|soc|ap|cpu|processor)$": "Chipset",  # processor 추가
                            r"^(ram|메모리)$": "RAM",
                            r"^(os|osversion|android|ios|펌웨어|소프트웨어버전)$": "OS",
                            r"^(rank|rating|ratinggrade|등급)$": "Rank",
                        }
                        for norm_name, orig_name in zip(norm_cols, original_cols):
                            mapped = None
                            for pat, std_name in synonyms.items():
                                if re.search(pat, norm_name):
                                    mapped = std_name; break
                            col_map[orig_name] = mapped or orig_name
                        return df.rename(columns=col_map)

                    def detect_model_col(df: pd.DataFrame):
                        if "Model" in df.columns:
                            return "Model"
                        for c in df.columns:
                            n = _norm_for_header(c)
                            if re.search(r"^(model|device|제품명|제품|모델명|모델)$", n):
                                return c
                        return None

                    def load_std_spec_df(xls, sheet):
                        hdr = find_header_row_for_spec(xls, sheet)
                        df = pd.read_excel(xls, sheet_name=sheet, header=hdr, engine="openpyxl")
                        df = standardize_columns(df)
                        model_col = detect_model_col(df)
                        if model_col is None:
                            raise ValueError(f"'{sheet}'에서 모델 컬럼을 찾지 못했습니다. 컬럼: {list(df.columns)}")
                        df["model_norm"] = df[model_col].apply(normalize_model_name_strict)
                        cols_keep = ["model_norm"]
                        for c in ["GPU", "제조사", "Chipset", "RAM", "OS", "Rank", "Model", "CPU"]:
                            if c in df.columns: cols_keep.append(c)
                        return df[cols_keep]

                    spec_frames = [load_std_spec_df(xls, s) for s in spec_sheets_selected]
                    df_spec_all = pd.concat(spec_frames, ignore_index=True)
                    df_spec_all = df_spec_all.drop_duplicates(subset=["model_norm"], keep="first")

                    df_final["model_norm"] = df_final["Device(Model)"].apply(normalize_model_name_strict)
                    df_final = pd.merge(df_final, df_spec_all, on="model_norm", how="left")

                    # CPU → Chipset 폴백
                    if "Chipset" not in df_final.columns and "CPU" in df_final.columns:
                        df_final["Chipset"] = df_final["CPU"]

                    # 접미사 정리
                    merge_cols = ["GPU", "제조사", "Chipset", "RAM", "OS", "Rank", "Model"]
                    for col in merge_cols:
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

        # 6) 자가진단
        with step_status("모듈 자가진단"):
            diag = self_check(df_final)
            diag_dump("self_check 결과", diag)
            if not diag["row_ok"]:
                st.error("❌ 유효한 데이터 없음. 중단.")
                st.stop()

        # 7) 코멘트 정규화 / 태깅
        with step_status("코멘트 정규화 / 태깅"):
            def _jamo_norm(s: str) -> str:
                if s is None: return ""
                t = unicodedata.normalize("NFKC", str(s))
                t = re.sub(r"[^0-9a-zA-Z가-힣\s\-_+/.:]", " ", t)
                t = re.sub(r"\s+", " ", t).strip().lower()
                return t

            ISSUE_TAG_PATTERNS = [
                ("punch_hole",   r"(펀치홀|punch[\s\-]?hole|hole[-\s]?camera)"),
                ("notch",        r"(노치|notch)"),
                ("rotation",     r"(회전|가로전환|세로전환|landscape|portrait|rotate)"),
                ("aspect_ratio", r"(화면비|비율|aspect\s?ratio)"),
                ("resolution",   r"(해상도|resolution)"),
                ("cutout",       r"(컷아웃|cutout)"),
                ("install",      r"(설치\s?불가|설치오류|install\s?fail|패키지\s?오류|apk\s?설치)"),
                ("permission",   r"(권한|permission)"),
                ("login",        r"(로그인|login|oauth|인증|auth)"),
                ("storage",      r"(저장공간|storage|sd\s?card|권한\s?거부)"),
                ("input_lag",    r"(입력\s?지연|지연\s?입력|터치\s?지연|ui\s?지연|input\s?lag|ui\s?lag)"),
                ("keyboard",     r"(키보드|ime|keyboard)"),
                ("ui_scaling",   r"(ui\s?스케일|확대|축소|dpi|density)"),
                ("render_artifact", r"(아티팩트|깨짐|잔상|테어링|글리치|artifact|glitch|tearing)"),
                ("black_screen", r"(검은\s?화면|black\s?screen)"),
                ("white_screen", r"(하얀\s?화면|white\s?screen)"),
                ("crash",        r"(크래시|fatal exception|강제종료|crash)"),
                ("network",      r"(네트워크|network|ssl|handshake|timeout|unknownhost)"),
                ("audio",        r"(소리|오디오|audio|무음|볼륨)"),
                ("camera",       r"(카메라|camera)"),
                ("thermal",      r"(써멀|발열|thermal|throttl)"),
                ("fps",          r"(프레임|fps)"),
            ]

            def tag_issue_comment(comment: str) -> list:
                s = _jamo_norm(comment)
                tags = []
                for tag, pat in ISSUE_TAG_PATTERNS:
                    if re.search(pat, s, re.I):
                        tags.append(tag)
                return list(dict.fromkeys(tags))

            if "comment_text" not in df_final.columns:
                df_final["comment_text"] = ""
            df_final["comment_norm"] = df_final["comment_text"].fillna("").astype(str).apply(_jamo_norm)
            df_final["issue_tags"]   = df_final["comment_text"].fillna("").astype(str).apply(tag_issue_comment)
            diag_dump("태깅 샘플", df_final[["Device(Model)","GPU","Chipset","OS","comment_text","issue_tags"]].head(15))

        # 8) 군집(Cluster) 통계 산출
        with step_status("군집(Cluster) 통계 산출"):
            # Chipset 폴백 사용
            if "Chipset" not in df_final.columns and "CPU" in df_final.columns:
                df_final["Chipset"] = df_final["CPU"]

            # 진단: 스펙 채움 비율
            gpu_fill = int(df_final.get("GPU", pd.Series([None]*len(df_final))).notna().sum()) if "GPU" in df_final.columns else 0
            chip_fill = int(df_final.get("Chipset", pd.Series([None]*len(df_final))).notna().sum()) if "Chipset" in df_final.columns else 0
            st.caption(f"🔎 GPU 채움: {gpu_fill}/{len(df_final)} · Chipset 채움: {chip_fill}/{len(df_final)}")

            if "GPU" not in df_final.columns:
                df_final["GPU"] = None
            if "Chipset" not in df_final.columns:
                df_final["Chipset"] = None

            def _cluster_counts(df, col, topn=15):
                if col not in df.columns:
                    return pd.DataFrame(columns=[col, "count"])
                vc = df[col].fillna("(미기재)").astype(str).str.strip().value_counts().head(topn)
                return vc.reset_index().rename(columns={"index": col, 0: "count"})

            if "GPU" in df_final.columns:
                df_final["GPU"] = (
                    df_final["GPU"].astype(str)
                    .str.replace(r"\bPower\s*VR\b", "PowerVR", regex=True)
                    .str.replace(r"\bIMG\s+GE", "PowerVR GE", regex=True)
                    .str.replace(r"\bGE(\d+)\b", r"PowerVR GE\1", regex=True)
                )

            cluster_gpu = _cluster_counts(df_final, "GPU")
            cluster_chip = _cluster_counts(df_final, "Chipset")

            clusters = {
                "by_gpu": cluster_gpu.to_dict(orient="records"),
                "by_chipset": cluster_chip.to_dict(orient="records"),
            }

            # Feature 군집(태그 기반)
            feat_rows = []
            for idx, r in df_final.iterrows():
                for t in (r.get("issue_tags") or []):
                    feat_rows.append({
                        "tag": t,
                        "row_idx": idx,
                        "device": str(r.get("Device(Model)", "")),
                        "gpu": str(r.get("GPU", "")),
                        "chipset": str(r.get("Chipset", "")),
                        "os": str(r.get("OS","")),
                        "comment": str(r.get("comment_text",""))
                    })
            feat_df = pd.DataFrame(feat_rows)
            clusters_feature_detailed = []
            by_issue_tag = []
            if not feat_df.empty:
                g = (feat_df.groupby("tag")
                            .agg(count=("row_idx","size"),
                                 repr_models=("device", lambda s: list(pd.Series(s).dropna().unique())[:3]),
                                 evidence_rows=("row_idx", list))
                            .sort_values("count", ascending=False)
                            .reset_index())
                by_issue_tag = g[["tag","count"]].rename(columns={"tag":"value"}).to_dict(orient="records")

                def _row_evidence(r):
                    return {
                        "row_idx": int(getattr(r, "name", -1)),
                        "device": str(r.get("Device(Model)", "")),
                        "os": str(r.get("OS", "")),
                        "comment": str(r.get("comment_text", ""))[:180]
                    }

                for _, row in g.iterrows():
                    ev = []
                    for ridx in row["evidence_rows"][:6]:
                        rr = df_final.loc[ridx]
                        ev.append(_row_evidence(rr))
                    clusters_feature_detailed.append({
                        "feature_tag": row["tag"],
                        "pattern": row["tag"],
                        "count": int(row["count"]),
                        "repr_models": row["repr_models"],
                        "evidence_rows": ev,
                        "singleton": (int(row["count"]) == 1)
                    })

            diag_dump("GPU/Chipset 군집", clusters)
            diag_dump("Feature 군집 요약", by_issue_tag)
            diag_dump("Feature 군집 상세(일부)", clusters_feature_detailed[:3])

            if cluster_gpu.empty and cluster_chip.empty:
                st.warning("GPU/Chipset 군집이 비어 있습니다. 스펙 병합 실패 가능성이 큽니다. "
                           "→ 테스트 시트에서 디바이스명이 제대로 채워지는지와, 스펙 시트 칼럼명이 Model/Chipset/GPU로 표준화되었는지 확인하세요.")

        # 8.5) gpt-4o 토큰 절감: 대표 샘플만 압축 추출
        def _compact_str(s, n=160):
            s = (str(s or "")).strip()
            return (s[:n] + "…") if len(s) > n else s

        def make_compact_sample(df: pd.DataFrame, per_tag=30, per_gpu=20, per_chip=20, max_rows=450):
            keep = [c for c in ["Sheet","Device(Model)","GPU","Chipset","OS","comment_text","issue_tags"] if c in df.columns]
            slim = df[keep].copy()
            slim["comment_text"] = slim["comment_text"].map(lambda x: _compact_str(x, 180))
            slim["__dedup_key__"] = (
                slim["Device(Model)"].astype(str).str.strip().str.lower()
                + "||" + slim["comment_text"].astype(str).str.strip().str.lower()
            )
            slim = slim.drop_duplicates("__dedup_key__")

            out = []
            if "issue_tags" in slim.columns:
                tag_order = ["crash","black_screen","white_screen","render_artifact","rotation",
                             "aspect_ratio","ui_scaling","resolution","permission","install",
                             "input_lag","fps","thermal","network","audio","camera","notch","punch_hole"]
                for t in tag_order:
                    sub = slim[slim["issue_tags"].astype(str).str.contains(t, regex=False, na=False)].head(per_tag)
                    out.append(sub)
            if "GPU" in slim.columns:
                for g in slim["GPU"].fillna("(미기재)").value_counts().head(10).index.tolist():
                    out.append(slim[slim["GPU"] == g].head(per_gpu))
            if "Chipset" in slim.columns:
                for c in slim["Chipset"].fillna("(미기재)").value_counts().head(10).index.tolist():
                    out.append(slim[slim["Chipset"] == c].head(per_chip))

            compact = pd.concat(out, ignore_index=True).drop_duplicates("__dedup_key__")
            compact = compact.head(max_rows).drop(columns=["__dedup_key__"], errors="ignore")
            return compact

        compact_issues = make_compact_sample(df_final, per_tag=30, per_gpu=20, per_chip=20, max_rows=450)

        # 9) 프롬프트 준비 + 토큰 예산 조정
        metrics = {
            "total_fail_issues": len(df_final),
            "clusters": clusters,
            "by_issue_tag": by_issue_tag,
            "clusters_feature_detailed": clusters_feature_detailed,
            "log_hypotheses": log_hypotheses
        }
        deltas, evidence_links = {}, []

        def _rough_token_count(t: str) -> int:
            return max(1, int(len(t) / 2.5))

        def estimate_tokens(msgs: list) -> int:
            try:
                import tiktoken
                enc = tiktoken.get_encoding("cl100k_base")
                return sum(len(enc.encode(m.get("content",""))) for m in msgs)
            except Exception:
                return sum(_rough_token_count(m.get("content","")) for m in msgs)

        def fit_prompt(build_user, base_kwargs, model_budget=30000, reserve_output=6000):
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

        base_kwargs = {
            "project": "UNKNOWN_PROJECT",
            "version": "UNKNOWN_VERSION",
            "metrics": metrics,
            "deltas": deltas,
            "evidence_links": evidence_links,
            "sample_issues": compact_issues,  # 핵심: 압축본 사용
            "max_rows": 500
        }
        with step_status("토큰 예산 조정"):
            sp, up, diag_budget = fit_prompt(build_user_prompt, base_kwargs)
            diag_dump("토큰 진단", diag_budget)

        # 10) OpenAI 호출
        with st.spinner("GPT가 리포트를 작성 중입니다... (429 오류 시 자동 재시도)"):
            max_retries = 3
            wait_time_seconds = 20
            last_error = None
            result = None

            for attempt in range(max_retries):
                try:
                    resp = client.chat.completions.create(
                        model="gpt-4o",
                        temperature=0.1,
                        top_p=0.9,
                        messages=[{"role":"system","content":sp},{"role":"user","content":up}],
                        response_format={"type": "json_object"}  # JSON 모드 강제
                    )
                    raw = resp.choices[0].message.content
                    result = parse_llm_json(raw)
                    result["metrics"] = metrics
                    diag_dump("LLM 원문(요약)", raw[:4000])
                    last_error = None
                    break

                except Exception as e:
                    last_error = e
                    error_message = str(e).lower()

                    if "rate_limit_exceeded" in error_message or "429" in error_message:
                        if attempt < max_retries - 1:
                            st.warning(f"⏳ RATE LIMIT (429) 감지 (시도 {attempt + 1}/{max_retries}). {wait_time_seconds}초 후 재시도합니다.")
                            time.sleep(wait_time_seconds)
                            wait_time_seconds *= 2
                        else:
                            st.error(f"❌ RATE LIMIT (429) 오류. 재시도({max_retries}회) 모두 실패.")
                            st.stop()
                    else:
                        st.error(f"❌ OpenAI 호출 중 복구 불가능한 오류 발생: {e}")
                        st.stop()

            if result is None:
                st.error(f"❌ OpenAI 호출 최종 실패: {last_error}")
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

