# -*- coding: utf-8 -*-
# Streamlit 앱: QA 결과 자동 코멘트 생성기 (최종)

import os, re, io, time, unicodedata
from contextlib import contextmanager
import pandas as pd
import openpyxl
import streamlit as st
from dotenv import load_dotenv
from openai import OpenAI
from typing import List, Dict, Any, Optional

# ============= 기본 설정 =============
load_dotenv()
st.set_page_config(page_title="QA 결과 자동 코멘트 생성기", layout="wide")
st.title(":bar_chart: QA 결과 자동 코멘트 생성기")

api_key = st.secrets.get("OPENAI_API_KEY", os.getenv("OPENAI_API_KEY", ""))
if not api_key:
    st.error("OpenAI API 키가 없습니다. st.secrets 또는 .env에 OPENAI_API_KEY를 설정하세요.")
    st.stop()
client = OpenAI(api_key=api_key)

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

# 세션 초기화
if st.button("🔄 세션 초기화"):
    st.session_state.clear()
    st.rerun()

st.caption("※ Logcat 분석은 현재 비활성화 상태입니다.")
uploaded_file = st.file_uploader("원본 QA 엑셀 파일을 업로드하세요", type=["xlsx"])

if not uploaded_file:
    st.stop()

# ============= 엑셀 로드 & 시트 선택 =============
data = uploaded_file.read()
with step_status("엑셀 로드"):
    xls = pd.ExcelFile(io.BytesIO(data), engine="openpyxl")
    diag_dump("시트 목록", xls.sheet_names)

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

st.subheader("2. 스펙 시트 선택 (디바이스 리스트)")
default_spec = [s for s in ["AOS_Device_List", "iOS_Device_List"] if s in xls.sheet_names]
spec_sheets_selected = st.multiselect(
    "스펙(Chipset, GPU, OS, Rank 등) 포함 시트 선택",
    options=xls.sheet_names,
    default=default_spec
)
st.markdown("---")

# ============= 실행 =============
if not st.button("분석 및 리포트 생성", type="primary"):
    st.stop()

log_hypotheses, clusters, evidence_links = [], {}, []

# 3) Fail + 코멘트 추출 (라벨행→Fail열 세로추출, 병합셀 보정)
with step_status("Fail + 셀 코멘트 추출"):
    wb = openpyxl.load_workbook(io.BytesIO(data), data_only=True)
    df_issue = extract_comments_as_dataframe(wb, test_sheets_selected)
    diag_dump("추출 샘플", df_issue.head(12))
    if df_issue.empty:
        st.warning("❌ Fail+코멘트 항목이 없습니다(셀 코멘트 기준).")
        st.stop()

# 4) 비고/Notes 병합 (선택 시트 전부)
with step_status("비고/Notes 병합"):
    for _sheet in test_sheets_selected:
        df_issue = enrich_with_column_comments(xls, _sheet, df_issue)
    diag_dump("비고 병합 결과", df_issue.head(12))

# 5) 스펙 병합 (모델명 정규화 후 Join)
df_final = df_issue.copy()
match_rate = 0.0
if spec_sheets_selected:
    with step_status("스펙 병합"):
        def _stdcols(df: pd.DataFrame) -> pd.DataFrame:
            orig = list(df.columns)
            norm = [unicodedata.normalize("NFKC", str(c)) for c in orig]
            norm = [re.sub(r"[\s\-\_/()\[\]{}:+·∙•]", "", s).lower() for s in norm]
            synonyms = {
                r"^(model|device|제품명|제품|모델명|모델)$": "Model",
                r"^(maker|manufacturer|brand|oem|제조사|벤더)$": "제조사",
                r"^(gpu|그래픽|그래픽칩|그래픽스|그래픽프로세서)$": "GPU",
                r"^(chipset|soc|ap|cpu|processor)$": "Chipset",
                r"^(ram|메모리)$": "RAM",
                r"^(os|osversion|android|ios|펌웨어|소프트웨어버전)$": "OS",
                r"^(rank|rating|ratinggrade|등급)$": "Rank",
            }
            col_map = {}
            for n, o in zip(norm, orig):
                mapped = None
                for pat, std in synonyms.items():
                    if re.search(pat, n):
                        mapped = std; break
                col_map[o] = mapped or o
            return df.rename(columns=col_map)

        frames = []
        for sname in spec_sheets_selected:
            dfp = pd.read_excel(xls, sheet_name=sname, engine="openpyxl")
            dfp = _stdcols(dfp)
            model_col = "Model" if "Model" in dfp.columns else None
            if not model_col:
                for c in dfp.columns:
                    n = re.sub(r"[\s\-\_/()\[\]{}:+·∙•]", "", unicodedata.normalize("NFKC", str(c))).lower()
                    if re.search(r"^(model|device|제품명|제품|모델명|모델)$", n): model_col = c; break
            if not model_col:
                continue
            dfp["model_norm"] = dfp[model_col].apply(normalize_model_name_strict)
            keep = ["model_norm"] + [c for c in ["GPU","제조사","Chipset","RAM","OS","Rank","Model","CPU"] if c in dfp.columns]
            frames.append(dfp[keep])

        if frames:
            df_spec_all = pd.concat(frames, ignore_index=True).drop_duplicates("model_norm", keep="first")
            df_final["model_norm"] = df_final["Device(Model)"].apply(normalize_model_name_strict)
            df_final = pd.merge(df_final, df_spec_all, on="model_norm", how="left")

            if "Chipset" not in df_final.columns and "CPU" in df_final.columns:
                df_final["Chipset"] = df_final["CPU"]

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

# 6) 자가진단
with step_status("모듈 자가진단"):
    diag = self_check(df_final)
    diag_dump("self_check", diag)
    if not diag["row_ok"]:
        st.error("❌ 유효한 데이터 없음. 중단.")
        st.stop()

# 7) 코멘트 정규화/태깅
with step_status("코멘트 정규화 / 태깅"):
    def _jamo_norm(s: str) -> str:
        if s is None: return ""
        t = unicodedata.normalize("NFKC", str(s))
        t = re.sub(r"[^0-9a-zA-Z가-힣\s\-_+/.:]", " ", t)
        t = re.sub(r"\s+", " ", t).strip().lower()
        return t

    ISSUE_TAG_PATTERNS = [
        ("punch_hole", r"(펀치홀|punch[\s\-]?hole|hole[-\s]?camera)"),
        ("notch", r"(노치|notch)"),
        ("rotation", r"(회전|가로전환|세로전환|landscape|portrait|rotate)"),
        ("aspect_ratio", r"(화면비|비율|aspect\s?ratio)"),
        ("resolution", r"(해상도|resolution)"),
        ("cutout", r"(컷아웃|cutout)"),
        ("install", r"(설치\s?불가|설치오류|install\s?fail|패키지\s?오류|apk\s?설치)"),
        ("permission", r"(권한|permission)"),
        ("login", r"(로그인|login|oauth|인증|auth)"),
        ("storage", r"(저장공간|storage|sd\s?card|권한\s?거부)"),
        ("input_lag", r"(입력\s?지연|터치\s?지연|ui\s?지연|input\s?lag|ui\s?lag)"),
        ("keyboard", r"(키보드|ime|keyboard)"),
        ("ui_scaling", r"(ui\s?스케일|확대|축소|dpi|density)"),
        ("render_artifact", r"(아티팩트|깨짐|잔상|테어링|글리치|artifact|glitch|tearing)"),
        ("black_screen", r"(검은\s?화면|black\s?screen)"),
        ("white_screen", r"(하얀\s?화면|white\s?screen)"),
        ("crash", r"(크래시|fatal exception|강제종료|crash)"),
        ("network", r"(네트워크|network|ssl|handshake|timeout|unknownhost)"),
        ("audio", r"(소리|오디오|audio|무음|볼륨)"),
        ("camera", r"(카메라|camera)"),
        ("thermal", r"(써멀|발열|thermal|throttl)"),
        ("fps", r"(프레임|fps)"),
    ]
    def tag_issue_comment(comment: str) -> list:
        s = _jamo_norm(comment)
        tags = []
        for tag, pat in ISSUE_TAG_PATTERNS:
            if re.search(pat, s, re.I): tags.append(tag)
        return list(dict.fromkeys(tags))

    if "comment_text" not in df_final.columns: df_final["comment_text"] = ""
    df_final["comment_norm"] = df_final["comment_text"].fillna("").astype(str).apply(_jamo_norm)
    df_final["issue_tags"]   = df_final["comment_text"].fillna("").astype(str).apply(tag_issue_comment)
    diag_dump("태깅 샘플", df_final[["Device(Model)","GPU","Chipset","OS","comment_text","issue_tags"]].head(15))

# 8) 군집 산출
with step_status("군집(Cluster) 통계 산출"):
    if "Chipset" not in df_final.columns and "CPU" in df_final.columns:
        df_final["Chipset"] = df_final["CPU"]

    if "GPU" not in df_final.columns: df_final["GPU"] = None
    if "Chipset" not in df_final.columns: df_final["Chipset"] = None

    df_final["GPU"] = (
        df_final["GPU"].astype(str)
        .str.replace(r"\bPower\s*VR\b", "PowerVR", regex=True)
        .str.replace(r"\bIMG\s+GE", "PowerVR GE", regex=True)
        .str.replace(r"\bGE(\d+)\b", r"PowerVR GE\1", regex=True)
    )

    def _cluster_counts(df, col, topn=15):
        if col not in df.columns: return pd.DataFrame(columns=[col,"count"])
        vc = df[col].fillna("(미기재)").astype(str).str.strip().value_counts().head(topn)
        return vc.reset_index().rename(columns={"index":col, 0:"count"})

    cluster_gpu  = _cluster_counts(df_final, "GPU")
    cluster_chip = _cluster_counts(df_final, "Chipset")

    clusters = {
        "by_gpu": cluster_gpu.to_dict(orient="records"),
        "by_chipset": cluster_chip.to_dict(orient="records"),
    }

    feat_rows = []
    for idx, r in df_final.iterrows():
        for t in (r.get("issue_tags") or []):
            feat_rows.append({
                "tag": t, "row_idx": idx,
                "device": str(r.get("Device(Model)", "")),
                "gpu": str(r.get("GPU", "")),
                "chipset": str(r.get("Chipset","")),
                "os": str(r.get("OS","")),
                "comment": str(r.get("comment_text",""))
            })
    feat_df = pd.DataFrame(feat_rows)
    clusters_feature_detailed, by_issue_tag = [], []
    if not feat_df.empty:
        g = (feat_df.groupby("tag")
                .agg(count=("row_idx","size"),
                     repr_models=("device", lambda s: list(pd.Series(s).dropna().unique())[:3]),
                     evidence_rows=("row_idx", list))
                .sort_values("count", ascending=False).reset_index())
        by_issue_tag = g[["tag","count"]].rename(columns={"tag":"value"}).to_dict(orient="records")
        for _, row in g.iterrows():
            ev = []
            for ridx in row["evidence_rows"][:6]:
                rr = df_final.loc[ridx]
                ev.append({
                    "row_idx": int(ridx),
                    "device": str(rr.get("Device(Model)","")),
                    "os": str(rr.get("OS","")),
                    "comment": str(rr.get("comment_text",""))[:180]
                })
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

# 8.5) 토큰 절감용 압축 샘플
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

# 9) 프롬프트 구성
metrics = {
    "total_fail_issues": len(df_final),
    "clusters": clusters,
    "by_issue_tag": by_issue_tag,
    "clusters_feature_detailed": clusters_feature_detailed,
    "log_hypotheses": log_hypotheses
}
deltas, evidence_links = {}, []

sp = build_system_prompt()
up = build_user_prompt(
    project="UNKNOWN_PROJECT",
    version="UNKNOWN_VERSION",
    metrics=metrics,
    deltas=deltas,
    evidence_links=evidence_links,
    sample_issues=compact_issues,
    max_rows=500
)

# 10) OpenAI 호출
with st.spinner("GPT가 리포트를 작성 중입니다..."):
    max_retries, wait = 3, 20
    result, last_error = None, None
    for attempt in range(max_retries):
        try:
            resp = client.chat.completions.create(
                model="gpt-4o",
                temperature=0.1,
                top_p=0.9,
                messages=[{"role":"system","content":sp},{"role":"user","content":up}],
                response_format={"type":"json_object"}
            )
            raw = resp.choices[0].message.content
            result = parse_llm_json(raw); result["metrics"] = metrics
            diag_dump("LLM 원문(요약)", raw[:3000])
            break
        except Exception as e:
            last_error = e
            if "429" in str(e) or "rate_limit_exceeded" in str(e).lower():
                if attempt < max_retries-1:
                    st.warning(f"429 감지, 재시도 {attempt+1}/{max_retries}")
                    time.sleep(wait); wait *= 2
                    continue
            st.error(f"OpenAI 호출 실패: {e}")
            st.stop()
    if result is None:
        st.error(f"OpenAI 최종 실패: {last_error}")
        st.stop()

# 11) 리포트 생성
try:
    output = "QA_Report.xlsx"
    write_excel_report(result, df_final, output)
    st.success("✅ 리포트 생성 완료")
    with open(output, "rb") as f:
        st.download_button("📊 Excel 리포트 다운로드", f.read(), file_name=output)
except Exception as e:
    st.error(f"리포트 생성 오류: {e}")

