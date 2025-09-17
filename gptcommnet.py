import re
import pandas as pd
import streamlit as st
from openai import OpenAI
import openpyxl
import io
import docx
import streamlit as st # st.secrets

# --- .env 파일에서 API 키 로드 ---
if "OPENAI_API_KEY" not in st.secrets:
    st.error("먼저 앱의 Settings > Secrets 메뉴에 OpenAI API 키를 설정해주세요.")
    st.stop() # 키가 없으면 여기서 앱 실행을 멈춤
client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

#==============================================================================
# 1. 메모 추출 및 데이터 처리 함수들 (변경 없음)
#==============================================================================
def find_row_by_labels(ws, labels, search_rows=30, search_cols=50):
    for r in range(1, search_rows + 1):
        for c in range(1, search_cols + 1):
            cell_value = ws.cell(row=r, column=c).value
            if cell_value and str(cell_value).strip() in labels:
                return r
    return 0

def get_checklist_label(ws, row):
    label_parts, columns_to_check = [], [6, 7, 9]
    for c in columns_to_check:
        for r_search in range(row, 0, -1):
            cell_value = ws.cell(row=r_search, column=c).value
            if cell_value and str(cell_value).strip():
                label_parts.append(str(cell_value).replace('\n', ' ').strip())
                break
    return " / ".join(label_parts)

def extract_comments_as_dataframe(wb, target_sheet_names):
    extracted_data = []
    for sheet_name in target_sheet_names:
        if sheet_name not in wb.sheetnames:
            st.warning(f"'{sheet_name}' 시트를 찾을 수 없습니다."); continue
        ws = wb[sheet_name]
        header_rows = {'Model': find_row_by_labels(ws, ['Model', '제품명']), 'Chipset': find_row_by_labels(ws, ['Chipset', 'CPU', 'AP']), 'RAM': find_row_by_labels(ws, ['RAM', '메모리']), 'Rank': find_row_by_labels(ws, ['Rating Grade?', 'Rank', '등급']), 'OS': find_row_by_labels(ws, ['OS Version', 'Android', 'iOS', 'OS'])}
        if header_rows['Model'] == 0: continue
        for row in ws.iter_rows():
            for cell in row:
                if cell.comment and str(cell.value).strip().lower() == 'fail':
                    r, c = cell.row, cell.column
                    device_info = {key: ws.cell(row=num, column=c).value if num > 0 else "" for key, num in header_rows.items()}
                    checklist = get_checklist_label(ws, r)
                    cleaned_comment = cell.comment.text.split("https://go.microsoft.com/fwlink/?linkid=870924.", 1)[-1].strip()
                    extracted_data.append({"Sheet": ws.title, "Device(Model)": device_info.get('Model', ''), "Chipset": device_info.get('Chipset', ''), "RAM": device_info.get('RAM', ''), "Rank": device_info.get('Rank', ''), "OS": device_info.get('OS', ''), "Checklist": checklist, "Comment(Text)": cleaned_comment})
    if not extracted_data: return None
    return pd.DataFrame(extracted_data)

def normalize_model_name(s):
    if pd.isna(s): return ""
    return re.sub(r"[\s\-_]+", "", str(s).strip().lower())

#==============================================================================
# 2. Streamlit 앱 로직
#==============================================================================
st.title(":bar_chart: QA 결과 자동 코멘트 생성기")
uploaded_file = st.file_uploader("원본 QA 엑셀 파일을 업로드하세요", type=["xlsx"])

if uploaded_file:
    xls = pd.ExcelFile(uploaded_file)
    sheet_names = xls.sheet_names
    st.info("파일에 포함된 시트 목록: " + ", ".join(sheet_names))

    st.markdown("---")
    st.subheader("1. 분석할 시트를 선택하세요")
    test_sheets_selected = st.multiselect(
        '메모를 추출할 테스트 시트를 모두 선택하세요',
        options=sheet_names
    )
    # <<< [수정됨] 상세 스펙 시트를 여러 개 선택할 수 있도록 변경 (selectbox -> multiselect)
    spec_sheets_selected = st.multiselect(
        'GPU 등 상세 정보가 포함된 스펙 시트를 모두 선택하세요 (AOS, IOS 등)',
        options=sheet_names
    )
    st.markdown("---")
    
    if st.button("분석 및 코멘트 생성 시작", type="primary"):
        wb = openpyxl.load_workbook(uploaded_file, data_only=True)
        df_issue = extract_comments_as_dataframe(wb, test_sheets_selected)

        if df_issue is not None:
            df_final = df_issue
            # <<< [수정됨] 여러 스펙 시트를 하나로 합치는 로직 추가
            if spec_sheets_selected:
                st.info(f"선택된 스펙 시트 {spec_sheets_selected}의 추가 정보를 병합합니다.")
                
                all_spec_dfs = []
                for sheet_name in spec_sheets_selected:
                    df_temp_spec = pd.read_excel(xls, sheet_name=sheet_name)
                    all_spec_dfs.append(df_temp_spec)
                
                # 모든 스펙 데이터프레임을 하나로 합침
                df_spec = pd.concat(all_spec_dfs, ignore_index=True)
                
                df_issue['model_norm'] = df_issue['Device(Model)'].apply(normalize_model_name)
                
                model_col_name = next((col for col in ['Model', '모델명', '제품명'] if col in df_spec.columns), None)
                if not model_col_name:
                     st.error("스펙 시트에 'Model' 또는 '모델명'/'제품명' 열이 없습니다."); st.stop()
                df_spec['model_norm'] = df_spec[model_col_name].apply(normalize_model_name)
                
                spec_cols_to_merge = ['model_norm']
                for col in ['GPU', '제조사']:
                    if col in df_spec.columns:
                        spec_cols_to_merge.append(col)
                
                # 중복된 모델명이 있을 경우를 대비해 중복 제거 (첫 번째 값 유지)
                df_spec = df_spec.drop_duplicates(subset=['model_norm'], keep='first')
                
                df_final = pd.merge(df_issue, df_spec[spec_cols_to_merge], on='model_norm', how='left')

            st.success(f"{len(df_final)}개의 'Fail' 항목 분석 준비 완료.")
            st.dataframe(df_final.head())
            
            # (이하 프롬프트 생성 및 GPT 호출 로직은 기존과 동일)
            issue_blocks, stats_text = [], []
            for _, row in df_final.iterrows():
                gpu_info = f" / GPU:{row.get('GPU','-')}" if 'GPU' in df_final.columns else ""
                manu_info = f" / 제조사:{row.get('제조사','-')}" if '제조사' in df_final.columns else ""
                issue_blocks.append(
                    f"- 기기: {row['Device(Model)']} / Chipset:{row.get('Chipset','-')} / RAM:{row.get('RAM','-')} / Rank:{row.get('Rank','-')}{gpu_info}{manu_info}\n"
                    f"  테스트 항목: {row['Checklist']}\n"
                    f"  이슈: {row['Comment(Text)']}"
                )
            formatted_issues = "\n".join(issue_blocks)
            
            def top_group_counts(df, key, topn=5):
                if key not in df.columns: return "N/A"
                vc = df[key].fillna("N/A").astype(str).str.strip().value_counts().head(topn)
                return "; ".join([f"{k}: {v}건" for k,v in vc.items()])
            
            stats_text = [
                f"제조사별 이슈 건수: {top_group_counts(df_final, '제조사')}",
                f"RAM별 이슈 건수: {top_group_counts(df_final, 'RAM')}",
                f"Rank별 이슈 건수: {top_group_counts(df_final, 'Rank')}",
                f"Chipset/SoC별 이슈 건수: {top_group_counts(df_final, 'Chipset')}",
                f"GPU별 이슈 건수: {top_group_counts(df_final, 'GPU')}",
            ]
            spec_cluster_summary = "\n".join([f"- {t}" for t in stats_text])
            
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
아래 '이슈 원문'과 '스펙 군집 통계'를 참고하여, 위 [출력 형식]에 맞춘 상세한 QA 리포트를 작성하라.
- 스펙 공통분모(RAM/Rank/Chipset/GPU)를 활용해 이슈를 군집화하고, 동일 유형은 하나의 항목으로 통합하라.
- 현상/영향/원인 추정/권고를 모두 포함하라.

[스펙 군지 통계]
{spec_cluster_summary}

[이슈 원문]
{formatted_issues}
"""
            with st.spinner("GPT가 리포트를 작성 중입니다..."):
                try:
                    resp = client.chat.completions.create(model="gpt-4o", temperature=0.2, messages=[{"role":"system", "content":SYSTEM_MSG}, {"role":"user", "content":USER_MSG}])
                    result = resp.choices[0].message.content
                    st.markdown("### :memo: 생성된 QA 코멘트")
                    st.markdown(result)
                    
                    output_buffer = io.BytesIO()
                    doc = docx.Document()
                    doc.add_paragraph(result)
                    doc.save(output_buffer)
                    
                    st.download_button(
                        label=":floppy_disk: 워드 파일로 코멘트 다운로드",
                        data=output_buffer.getvalue(),
                        file_name="QA_Comment.docx",
                        mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                    )
                except Exception as e:

                    st.error(f"OpenAI API 호출 중 오류가 발생했습니다: {e}")

