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
    # 시트 목록 안내
    xls = pd.ExcelFile(uploaded_file, engine="openpyxl")
    sheet_names = xls.sheet_names
    st.info("파일 포함 시트: " + ", ".join(sheet_names))

    # 기본 선택(질문에 주신 시트명)
    default_test = [s for s in ["Compatibility Test(AOS)", "Compatibility Test(iOS)"] if s in sheet_names]
    default_spec = [s for s in ["AOS_Device_List", "iOS_Device_List"] if s in sheet_names]

    st.markdown("---")
    st.subheader("1. 분석할 시트를 선택하세요")
    test_sheets_selected = st.multiselect(
        "메모를 추출할 테스트 시트를 모두 선택",
        options=sheet_names,
        default=default_test
    )

    st.subheader("2. 스펙 시트를 선택하세요 (AOS, iOS)")
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

        # 2) 스펙 병합
        df_final = df_issue.copy()
        match_rate = 0.0
        if spec_sheets_selected:
            st.info(f"선택된 스펙 시트 {spec_sheets_selected}의 추가 정보를 병합합니다.")
            try:
                spec_frames = [load_std_spec_df(xls, s) for s in spec_sheets_selected]
                df_spec_all = pd.concat(spec_frames, ignore_index=True)
                # 같은 model_norm의 중복은 첫 번째 유지
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

        # 3) 통계/요약
        stats_text = [
            f"제조사별 이슈 건수: {top_group_counts(df_final, '제조사')}",
            f"RAM별 이슈 건수: {top_group_counts(df_final, 'RAM')}",
            f"Rank별 이슈 건수: {top_group_counts(df_final, 'Rank')}",
            f"Chipset/SoC별 이슈 건수: {top_group_counts(df_final, 'Chipset')}",
            f"GPU별 이슈 건수: {top_group_counts(df_final, 'GPU')}",
        ]
        spec_cluster_summary = "\n".join([f"- {t}" for t in stats_text])

        # 4) GPT 프롬프트 생성
        issue_blocks = []
        has_gpu = "GPU" in df_final.columns
        has_manu = "제조사" in df_final.columns

        for _, row in df_final.iterrows():
            gpu_info = f" / GPU:{row.get('GPU','-')}" if has_gpu else ""
            manu_info = f" / 제조사:{row.get('제조사','-')}" if has_manu else ""
            issue_blocks.append(
                f"- 기기: {row.get('Device(Model)','-')} / Chipset:{row.get('Chipset','-')} / RAM:{row.get('RAM','-')} / Rank:{row.get('Rank','-')}{gpu_info}{manu_info}\n"
                f"  테스트 항목: {row.get('Checklist','-')}\n"
                f"  이슈: {row.get('Comment(Text)','-')}"
            )
        formatted_issues = "\n".join(issue_blocks)

        SYSTEM_MSG = """
너는 숙련된 QA 리더다. 모든 출력은 한국어, 보고서 톤(정중/전문/단정)으로 작성한다.
입력은 JSON 형식의 이슈 데이터 배열이다. 각 객체는 device_model, manufacturer, cpu , gpu, chipset, ram, os, rank, issue_comment 등을 가진다.

너의 임무는 이 데이터를 분석하여 **전략적인 품질 분석 리포트용 코멘트**를 작성하는 것이다.
작성된 결과는 QA 팀 내부 공유뿐 아니라 기획자/개발자/경영진 보고에도 활용되므로 다음 규칙을 반드시 지켜라.
테스트 결과를 바탕으로 **전략적인 품질 분석 리포트용 코멘트**를 작성해야 한다.
---
[분석 규칙]
1. **사실 기반**: 입력에 없는 사실·기기·수치·비율은 절대 언급하지 않는다.
2. **인과관계 추론**: issue_comment와 cpu/gpu/chipset/ram/manufacturer/os/rank를 연관지어 원인을 추정한다.
3. **패턴 식별 및 군집화**:
   - 동일/유사 cpu, gpu, chipset, manufacturer, ram 범위를 가진 기기에서 유사 issue_comment가 반복되면 하나의 항목으로 묶는다.
   - 반드시 근거(모델명, GPU, RAM 등)와 기기 수를 명시한다.
4. **GPU/Chipset 급 분류**:
   - 그래픽 관련 이슈는 해당 GPU/Chipset이 고사양/중사양/저사양 중 어디에 속하는지 분석한다.
   - 분류 가이드:
     - 고사양: Snapdragon 8 Gen, Dimensity 9000 계열, Adreno 7xx 최신, Mali-G78/G710 이상
     - 중사양: Snapdragon 6xx/7xx, Dimensity 700~800, Adreno 6xx 다수, Mali-G57/G68 등
     - 저사양: RAM 2~4GB, Adreno 5xx, Mali-G52/G51, PowerVR GE 계열, Unisoc/Tiger 하위
   - 정확히 구분 어려우면 “보급형/중급형 추정”으로 표기한다.
5. **표현 범주**: issue_comment를 FailCommentExport 유형에 맞춰 표현한다.
   - “크래시/강제 종료/앱 종료/ANR”
   - “FPS/프레임/발열/스로틀링/로딩 지연”
   - “펀치홀/레터박스/노치/가림/겹침/깜빡임”
   - “로그인/네트워크/접속 불가”
   - “그래픽/텍스처/픽셀/비정상 출력”
   - “입력/터치 미동작”
6. **현상 → 영향 → 원인 추정 → 권고** 순으로 작성한다.
7. **수치 표기**: 기기 수가 명확히 입력에 있을 때만 "N대 중 M대" 형태로 표기한다. 없으면 "다수/일부 기기"로 표기한다.
8. **문체 규칙**:
   - “확인되었으며”, “재현됨”, “분석됩니다” 등 보고서 톤 사용.
   - 중복 문장 피하고 간결하게 작성.
   - 불확실한 부분은 “추정/가능성/추가 확인 필요”로 기술한다.

[출력 스캐폴드]
■ 주요 이슈 분석
1. **[이슈 요약 제목]**
   - **현상**: 발생한 문제 현상을 명확히 기술.
   - **발생 기기**: 이슈가 발생한 기기 모델 목록과 공통된 스펙(GPU, 칩셋, RAM, manufacture, cpu 등)을 명시.
   - **영향**: 이 문제가 사용자 경험에 미치는 영향.
   - **원인 추정**: 데이터에 근거하여 기술적인 원인을 심도 있게 추정.

[Summary & Insight]
- **주요 패턴**: 이번 테스트에서 발견된 두드러진 이슈 패턴(예: "저사양 AP 기기군의 그래픽 문제", "특정 제조사의 호환성 문제").
- 공통 원인 또는 군집 기반 통계
- **핵심 문제 식별**: 여러 이슈 중 가장 시급하게 해결해야 할 단 하나의 핵심 문제를 명시하고 그 이유를 설명.
- **대응 우선순위**: 그 외 이슈들에 대한 대응 우선순위 제안.
- **종합 의견**: 현재 빌드의 전반적인 평가 및 각 이슈별 사용자 체감 평가

[문체 규칙]
- 반드시 **한글**로 작성
- 논리적, 보고서 스타일 문장 사용 (예: "확인되었으며", "필요합니다", "분석됩니다")
- "문제다"보다는 "문제가 재현되었으며", "확인이 필요함" 등 **조사형 보고문 표현**
- 항목 간 반복 표현은 피하고, 문장은 간결하게
"""

        USER_MSG = f"""
JSON 형식의 '이슈 데이터'를 [분석 규칙]과 [출력 스캐폴드]에 따라 분석하여 상세한 QA 리포트를 작성하라.
- 스펙 공통분모(RAM/Rank/Chipset/GPU)를 활용해 이슈를 군집화하고, 동일 유형은 하나의 항목으로 통합하라.
- 현상/영향/원인 추정/권고를 모두 포함하라.

[스펙 군지 통계]
{spec_cluster_summary}

[이슈 원문]
{formatted_issues}
"""

        # 5) GPT 호출
        with st.spinner("GPT가 리포트를 작성 중입니다..."):
            try:
                resp = client.chat.completions.create(
                    model="gpt-4o",
                    temperature=0.2,
                    messages=[
                        {"role": "system", "content": SYSTEM_MSG},
                        {"role": "user", "content": USER_MSG},
                    ],
                )
                result_text = resp.choices[0].message.content
            except Exception as e:
                st.error(f"OpenAI API 호출 중 오류가 발생했습니다: {e}")
                st.stop()

        # 6) 출력 및 다운로드
        st.markdown("### :memo: 생성된 QA 코멘트")
        st.markdown(result_text)

        # DOCX 저장
        try:
            output_buffer = io.BytesIO()
            doc = docx.Document()
            for block in result_text.split("\n"):
                doc.add_paragraph(block)
            doc.save(output_buffer)

            st.download_button(
                label=":floppy_disk: 워드 파일로 코멘트 다운로드",
                data=output_buffer.getvalue(),
                file_name="QA_Comment.docx",
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            )
        except Exception as e:
            st.warning(f"DOCX 저장 중 경고: {e}")

        # 7) (선택) 매칭 실패 샘플/디버그 가시화
        with st.expander("디버그/점검 정보"):
            st.write(f"스펙 매칭률: {match_rate}%")
            if "GPU" in df_final.columns:
                unmatched = df_final[df_final["GPU"].isna()]
                if not unmatched.empty:
                    st.write("스펙 매칭 실패 사례(상위 10개):")
                    st.dataframe(unmatched.head(10), use_container_width=True)
