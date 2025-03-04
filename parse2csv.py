import json
import csv
import glob
import os
from typing import List, Dict
import pandas as pd


def read_json_file(file_path: str) -> List[Dict]:
    """JSON 파일을 읽어서 데이터를 반환합니다."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return []


def flatten_data(item: Dict) -> Dict:
    """중첩된 JSON 데이터를 평탄화합니다."""
    flattened = {}

    # 인증정보
    cert_info = item.get("인증정보", {})
    flattened.update(
        {
            "인증번호": cert_info.get("인증번호", ""),
            "인증기관": cert_info.get("인증기관", ""),
            "인증구분": cert_info.get("인증구분", ""),
            "인증상태": cert_info.get("인증상태", ""),
            "인증일자": cert_info.get("인증일자", ""),
            "인증변경일자": cert_info.get("인증변경일자", ""),
            "인증변경사유": cert_info.get("인증변경사유", ""),
            "리콜현황(모델명)": cert_info.get("리콜현황(모델명)", ""),
        }
    )

    # 제품정보
    product_info = item.get("제품정보", {})
    flattened.update(
        {
            "품목명": product_info.get("품목명", ""),
            "모델명": product_info.get("모델명", ""),
            "상세정보": product_info.get("상세정보", ""),
            "제품분류코드": product_info.get("제품분류코드", ""),
            "파생모델": product_info.get("파생모델", ""),
        }
    )

    # 제조공장과 연관 인증 번호는 JSON 문자열로 저장
    flattened["제조공장"] = json.dumps(item.get("제조공장", []), ensure_ascii=False)
    flattened["연관인증번호"] = json.dumps(item.get("연관 인증 번호", []), ensure_ascii=False)

    return flattened


def clean_factory_name(name: str) -> str:
    """제조공장명을 정제합니다."""
    cleaned = name.replace(".", "").upper().replace(",", " ")
    cleaned = " ".join(cleaned.split())
    return cleaned


def extract_chinese_factories(df: pd.DataFrame) -> pd.DataFrame:
    """중국 제조 공장 데이터를 추출합니다."""
    # 결과를 저장할 리스트
    factory_data = []

    # 각 행을 순회하며 중국 제조 공장 정보 추출
    for _, row in df.iterrows():
        cert_number = row["인증번호"]
        factories = json.loads(row["제조공장"])

        # 제조공장 리스트에서 중국 공장만 필터링
        chinese_factories = [factory for factory in factories if factory.get("제조국", "").strip() == "중국"]

        # 중국 공장이 있는 경우만 데이터 추가
        if chinese_factories:
            for factory in chinese_factories:
                # 제조공장명 정제
                factory_name = clean_factory_name(factory.get("제조공장", ""))

                factory_data.append(
                    {
                        "인증번호": cert_number,
                        "제조공장번호": factory.get("번호", ""),
                        "제조공장명": factory_name,
                        "제조국": factory.get("제조국", ""),
                        "품목명": row["품목명"],
                        "모델명": row["모델명"],
                        "인증상태": row["인증상태"],
                        "인증일자": row["인증일자"],
                        "인증변경일자": row["인증변경일자"],
                    }
                )

    # 데이터프레임 생성
    factory_df = pd.DataFrame(factory_data)
    return factory_df


def main():
    # CSV 헤더 정의
    headers = [
        "인증번호",
        "인증기관",
        "인증구분",
        "인증상태",
        "인증일자",
        "인증변경일자",
        "인증변경사유",
        "리콜현황(모델명)",
        "품목명",
        "모델명",
        "상세정보",
        "제품분류코드",
        "파생모델",
        "제조공장",
        "연관인증번호",
    ]

    # 모든 데이터를 저장할 리스트
    all_data = []

    # output 디렉토리의 모든 JSON 파일 읽기
    json_files = glob.glob("output_bak/*.json")

    # 각 JSON 파일 처리
    for file_path in json_files:
        print(f"Processing {file_path}...")
        data = read_json_file(file_path)

        # 각 항목을 평탄화하여 저장
        for item in data:
            flattened = flatten_data(item)
            all_data.append(flattened)

    # 데이터프레임으로 변환
    df = pd.DataFrame(all_data)

    # 처리 전 데이터 수 저장
    total_before = len(df)

    # 중복 제거 (모든 컬럼 값이 동일한 행 제거)
    df_no_duplicates = df.drop_duplicates()

    # 인증번호 기준 중복 제거 (가장 최근 데이터 유지)
    df_unique = df_no_duplicates.sort_values("인증변경일자", ascending=False).drop_duplicates(
        subset=["인증번호"], keep="first"
    )

    # 중복 제거 결과 출력
    total_after_full_dup = len(df_no_duplicates)
    total_after_cert_dup = len(df_unique)

    print(f"\n중복 제거 결과:")
    print(f"- 원본 데이터: {total_before}개")
    print(f"- 완전 중복 제거 후: {total_after_full_dup}개 (제거된 행: {total_before - total_after_full_dup}개)")
    print(
        f"- 인증번호 기준 중복 제거 후: {total_after_cert_dup}개 (추가 제거된 행: {total_after_full_dup - total_after_cert_dup}개)"
    )

    # CSV 파일로 저장
    output_file = "output.csv"
    df_unique.to_csv(output_file, index=False, encoding="utf-8-sig")

    print(f"\n처리 완료! 최종 {total_after_cert_dup}개의 unique 데이터가 {output_file}에 저장되었습니다.")

    # 중국 제조 공장 데이터 추출 및 저장
    factory_df = extract_chinese_factories(df_unique)
    factory_file = "factory.csv"
    factory_df.to_csv(factory_file, index=False, encoding="utf-8-sig")

    # 고유한 제조공장명 수 계산
    unique_factory_names = factory_df["제조공장명"].nunique()

    print(f"\n중국 제조 공장 데이터 처리 결과:")
    print(f"- 전체 인증 건수: {len(df_unique)}개")
    print(f"- 중국 제조공장수: {unique_factory_names}개")
    print(f"- 저장 완료: {factory_file}")


if __name__ == "__main__":
    main()
