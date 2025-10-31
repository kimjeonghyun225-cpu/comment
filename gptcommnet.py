# -*- coding: utf-8 -*-
# 최종 Streamlit 앱: QA 결과 자동 코멘트 생성기 (헤더자동탐지/동의어매핑/모델정규화 적용)

import os
import re
import io
import unicodedata
import pandas as pd
import openpyxl
import docx
import streamlit as st
from dotenv import load_dotenv
from openai import OpenAI
from qa_patch_module import (
    find_test_sheet_candidates,
    enrich_with_column_comments,
    build_system_prompt, build_user_prompt,
    parse_llm_json, write_excel_report
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

# =========================
# 공통 유틸
# =========================
def _norm(s: str) -> str:
    """문자열 정규화: NFKC → 특수문자 제거 → 소문자/strip"""
    s = unicodedata.normalize("NFKC", str(s))
    s = re.sub(r"[\s\-\_/()\[\]{}:+·∙•]", "", s)
    return s.lower().strip()

def normalize_model_name_strict(s):
    """모델명 정규화: 괄호/용량/색상/구분자 제거 후 소문자/무공백."""
    if pd.isna(s):
        return ""
    s = str(s)
    s = re.sub(r"\(.*?\)", "", s)  # 괄호 내용 제거
    s = re.sub(r"\b(64|128|256|512)\s*gb\b", "", s, flags=re.I)  # 용량 제거
    s = re.sub(r"\b(black|white|blue|red|green|gold|silver|골드|블랙|화이트|실버)\b", "", s, flags=re.I)  # 색상 제거(확장 가능)
    s = re.sub(r"[\s\-_]+", "", s)  # 공백/하이픈/언더스코어 제거
    return s.lower().strip()

# =========================
# 분석 시트: 헤더 탐색 및 Fail+코멘트 추출
# =========================
def find_row_by_labels(ws, labels, search_rows=30, search_cols=70):
    """
    분석 시트 상단에서 주어진 라벨(복수) 중 하나가 등장하는 '행 번호'를 반환.
    동일 컬럼 c에서 장비 스펙을 가져오기 위함.
    """
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
    """
    Fail 셀의 행(row) 기준, 상단으로 올라가며 지정된 컬럼들에서 항목 라벨을 구성.
    시트마다 다단 헤더/중간 제목/병합 구조를 견딜 수 있게 설계.
    """
    label_parts, columns_to_check = [], [6, 7, 9]  # 필요 시 조정
    for c in columns_to_check:
        for r_search in range(row, 0, -1):
            cell_value = ws.cell(row=r_search, column=c).value
            if cell_value and str(cell_value).strip():
                label_parts.append(str(cell_value).replace("\n", " ").strip())
                break
    return " / ".join(label_parts)

def extract_comments_as_dataframe(wb, target_sheet_names):
    """
    분석 시트들에서 'fail'값 + 코멘트가 존재하는 셀만 추출하여 DF로 반환.
    컬럼: Sheet, Device(Model), Chipset, RAM, Rank, OS, Checklist, Comment(Text)
    """
    extracted = []
    for sheet_name in target_sheet_names:
        if sheet_name not in wb.sheetnames:
            st.warning(f"'{sheet_name}' 시트를 찾을 수 없습니다.")
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
                    # MS 링크 꼬리표 제거
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
                        "Comment(Text)": comment_text,
                    })

    if not extracted:
        return None
    return pd.DataFrame(extracted)

# =========================
# 스펙 시트: 헤더 자동탐지/컬럼 정규화/동의어 매핑
# =========================
def find_header_row_for_spec(xls, sheet_name, max_scan_rows=12):
    """
    스펙 시트 상단 N행을 훑어 Model/제품명/모델명/제품/Device 등 패턴이 보이는 행을 헤더로 결정.
    없으면 0(첫 행) 반환.
    """
    df_probe = pd.read_excel(xls, sheet_name=sheet_name, header=None, engine="openpyxl")
    header_row_idx = 0
    header_candidates = [r"^model$", r"^device$", r"^제품명$", r"^제품$", r"^모델명$", r"^모델$"]
    for r in range(min(max_scan_rows, len(df_probe))):
        row_vals = df_probe.iloc[r].astype(str).fillna("")
        norm_vals = [_norm(v) for v in row_vals]
        for v in norm_vals:
            if any(re.search(pat, v) for pat in header_candidates):
                return r
    return header_row_idx

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    컬럼명을 정규화하고 한글/영문 동의어를 표준 컬럼으로 매핑.
    """
    original_cols = list(df.columns)
    norm_cols = [_norm(c) for c in original_cols]
    col_map = {}

    synonyms = {
        # 모델
        r"^(model|device|제품명|제품|모델명|모델)$": "Model",
        # 제조사
        r"^(maker|manufacturer|brand|oem|제조사|벤더)$": "제조사",
        # GPU
        r"^(gpu|그래픽|그래픽칩|그래픽스|그래픽프로세서)$": "GPU",
        # 칩셋/CPU
        r"^(chipset|soc|ap|cpu)$": "Chipset",
        # RAM
        r"^(ram|메모리)$": "RAM",
        # OS
        r"^(os|osversion|android|ios|펌웨어|소프트웨어버전)$": "OS",
        # 등급
        r"^(rank|rating|ratinggrade|등급)$": "Rank",
    }

    for norm_name, orig_name in zip(norm_cols, original_cols):
        mapped = None
        for pat, std_name in synonyms.items():
            if re.search(pat, norm_name):
                mapped = std_name
                break
        if mapped is None:
            mapped = orig_name
        col_map[orig_name] = mapped

    return df.rename(columns=col_map)

def detect_model_col(df: pd.DataFrame):
    if "Model" in df.columns:
        return "Model"
    for c in df.columns:
        n = _norm(c)
        if re.search(r"^(model|device|제품명|제품|모델명|모델)$", n):
            return c
    return None

def load_std_spec_df(xls, sheet):
    """
    스펙 시트를: 헤더자동탐지 → 표준컬럼 매핑 → model_norm 생성 → 병합대상 컬럼만 추출
    """
    hdr = find_header_row_for_spec(xls, sheet)
    df = pd.read_excel(xls, sheet_name=sheet, header=hdr, engine="openpyxl")
    df = standardize_columns(df)
    model_col = detect_model_col(df)
    if model_col is None:
        raise ValueError(f"'{sheet}'에서 모델 컬럼을 찾지 못했습니다. 컬럼: {list(df.columns)}")
    df["model_norm"] = df[model_col].apply(normalize_model_name_strict)

    cols_keep = ["model_norm"]
    for c in ["GPU", "제조사", "Chipset", "RAM", "OS", "Rank", "Model"]:
        if c in df.columns:
            cols_keep.append(c)
    return df[cols_keep]

# =========================
# 통계/요약 유틸
# =========================
def top_group_counts(df, key, topn=5):
    if key not in df.columns:
        return "N/A"
    vc = df[key].fillna("N/A").astype(str).str.strip().value_counts().head(topn)
    return "; ".join([f"{k}: {v}건" for k, v in vc.items()])

# =========================
# UI: 파일 업로드
# =========================
uploaded_file = st.file_uploader("원본 QA 엑셀 파일을 업로드하세요", type=["xlsx"])

if uploaded_file:
    # 시트 자동 감지용 Excel 객체 생성
    xls = pd.ExcelFile(uploaded_file, engine="openpyxl")

    # ✅ [패치 모듈 사용] 테스트 시트 자동 후보 감지 + 선택
    test_candidates = find_test_sheet_candidates(xls)
    st.subheader("1. 테스트 시트 선택 (AOS/iOS 각각 1개 이상 권장)")
    test_sheets_selected = st.multiselect(
        "테스트 시트를 선택하세요 (팀마다 시트명이 달라도 자동 감지됩니다)",
        options=test_candidates,
        default=test_candidates[:2]  # 자동 후보 중 2개 기본 선택
    )

    if not test_sheets_selected:
        st.error("❌ 최소 1개 이상의 테스트 시트를 선택해야 합니다.")
        st.stop()

    # ✅ 기존 스펙 시트 선택 부분은 유지 (내부 병합용)
    sheet_names = xls.sheet_names
    default_spec = [s for s in ["AOS_Device_List", "iOS_Device_List"] if s in sheet_names]
    st.subheader("2. 스펙 시트 선택 (AOS/iOS 디바이스 리스트)")
    spec_sheets_selected = st.multiselect(
        "GPU/제조사/Chipset/RAM/OS/Rank 등 추가 정보가 포함된 시트",
        options=sheet_names,
        default=default_spec
    )
    st.markdown("---")

    if st.button("분석 및 코멘트 생성 시작", type="primary"):
        # 1) 분석(Fail+코멘트 추출)
        wb = openpyxl.load_workbook(uploaded_file, data_only=True)
        df_issue = extract_comments_as_dataframe(wb, test_sheets_selected)
        if df_issue is None or df_issue.empty:
            st.warning("Fail + 코멘트가 포함된 항목을 찾지 못했습니다.")
            st.stop()

        # 비고/Notes/Comment 열까지 병합 (키 컬럼은 여러분 시트 구조에 맞춰 조정 가능)
        df_issue = enrich_with_column_comments(
            xls, 
            test_sheets_selected[0], 
            df_issue, 
            key_cols=["Checklist", "Device(Model)"]
        )

        # 2) 스펙 병합 (선택된 내부 스펙 시트)
        df_final = df_issue.copy()
        match_rate = 0.0
        if spec_sheets_selected:
            st.info(f"선택된 스펙 시트 {spec_sheets_selected}의 추가 정보를 병합합니다.")
            try:
                spec_frames = [load_std_spec_df(xls, s) for s in spec_sheets_selected]
                df_spec_all = pd.concat(spec_frames, ignore_index=True)
                df_spec_all = df_spec_all.drop_duplicates(subset=["model_norm"], keep="first")

                df_final["model_norm"] = df_final["Device(Model)"].apply(normalize_model_name_strict)
                df_final = pd.merge(df_final, df_spec_all, on="model_norm", how="left")

                if "GPU" in df_final.columns:
                    matched = int(df_final["GPU"].notna().sum())
                    match_rate = round(matched / len(df_final) * 100, 1)
                    st.success(f"스펙 매칭 결과: {matched} / {len(df_final)} 건 ({match_rate}%)")
            except Exception as e:
                st.error(f"스펙 병합 중 오류: {e}")

        st.success(f"{len(df_final)}개의 'Fail' 항목 분석 준비 완료.")
        st.dataframe(df_final.head(15), use_container_width=True)

        # 3) 간단 통계 → metrics_summary (LLM 입력용)
        def vc_topn(series, n=5):
            return series.fillna("N/A").astype(str).str.strip().value_counts().head(n).to_dict()

        metrics_summary = {
            "fail_count": int(len(df_final)),
            "by_gpu": vc_topn(df_final["GPU"]) if "GPU" in df_final.columns else {},
            "by_chipset": vc_topn(df_final["Chipset"]) if "Chipset" in df_final.columns else {},
            "by_ram": vc_topn(df_final["RAM"]) if "RAM" in df_final.columns else {},
            "by_rank": vc_topn(df_final["Rank"]) if "Rank" in df_final.columns else {},
            "by_maker": vc_topn(df_final["제조사"]) if "제조사" in df_final.columns else {},
        }

        # 4) LLM 프롬프트 구성 (JSON 강제)
        system_prompt = build_system_prompt()
        user_prompt = build_user_prompt(metrics_summary, df_final)  # df_issue도 가능하나, 스펙 병합된 df_final 권장

        # (선택) 출력 전 형식 시뮬레이션: 실제 LLM 입력 미리보기
        with st.expander("📤 LLM 입력 프리뷰 (출력 전 시뮬레이션)"):
            st.code(user_prompt, language="json")

        # ============================================
        # 5) GPT 호출 + JSON 결과 파싱 + Excel 리포트 생성
        # ============================================
        with st.spinner("GPT가 리포트를 작성 중입니다..."):
            try:
                resp = client.chat.completions.create(
                    model="gpt-4o",        # 필요 시 gpt-4.1-mini 등으로 조정
                    temperature=0.2,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                )

                # JSON 결과 파싱
                result_json = parse_llm_json(resp.choices[0].message.content)

                # Excel 리포트 생성 (스펙 병합본 df_final 사용 권장)
                output_path = "QA_Report.xlsx"
                write_excel_report(result_json, df_final, output_path)

                st.success(f"✅ AI 분석 완료! 결과 리포트가 '{output_path}'로 생성되었습니다.")
                with open(output_path, "rb") as f:
                    st.download_button(
                        label="📊 Excel 리포트 다운로드",
                        data=f.read(),
                        file_name=output_path,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )

            except Exception as e:
                st.error(f"OpenAI API 호출 중 오류가 발생했습니다: {e}")
                st.stop()

        # 6) (선택) 매칭 실패 샘플/디버그 가시화
        with st.expander("디버그/점검 정보"):
            st.write(f"스펙 매칭률: {match_rate}%")
            if "GPU" in df_final.columns:
                unmatched = df_final[df_final["GPU"].isna()]
                if not unmatched.empty:
                    st.write("스펙 매칭 실패 사례(상위 10개):")
                    st.dataframe(unmatched.head(10), use_container_width=True)
