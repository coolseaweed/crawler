import pandas as pd
import json
import glob
import os


def clean_factory_name(name: str) -> str:
    """제조공장명을 정제합니다."""
    if name is None:  # None 체크 추가
        return ""  # None일 경우 빈 문자열 반환
    cleaned = name.replace(".", "").upper().replace(",", " ")
    cleaned = " ".join(cleaned.split())
    return cleaned


# JSON 파일 경로와 저장할 CSV 파일 경로 설정
json_files = glob.glob("data/kc/*.json")  # data/kc 디렉토리의 모든 JSON 파일
csv_file = "data/kc/combined_certifications.csv"  # 출력할 CSV 파일 이름
chinese_csv_file = "data/kc/chinese_factories.csv"  # 중국 제조공장만 저장할 CSV 파일 이름

# 모든 데이터를 저장할 리스트
all_data = []

# 각 JSON 파일 처리
for json_file in json_files:
    print(f"Processing {json_file}...")
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)
        all_data.extend(data)  # JSON 파일의 데이터를 리스트에 추가

# DataFrame으로 변환
df = pd.json_normalize(all_data)

# makerName 클렌징
df["makerName"] = df["makerName"].apply(clean_factory_name)

# 중복 제거 (인증번호 기준으로 중복 제거)
df_unique = df.drop_duplicates(subset=["certNum"], keep="first")

# "makerCntryName"이 "중국"인 데이터 필터링
df_chinese = df_unique[df_unique["makerCntryName"] == "중국"]

# DataFrame을 CSV 파일로 저장 (인덱스 없이, utf-8-sig 인코딩으로 저장)
df_unique.to_csv(csv_file, index=False, encoding="utf-8-sig")
df_chinese.to_csv(chinese_csv_file, index=False, encoding="utf-8-sig")
unique_factory_names = df_chinese["makerName"].nunique()


print(f"CSV 파일이 '{csv_file}'로 저장되었습니다.")
print(f"중국 제조공장 데이터가 '{chinese_csv_file}'로 저장되었습니다.")
print(f"* 총 KC 인증 데이터 수: {len(df_unique)}개")
print(f"* 총 중국 제조 데이터 수: {len(df_chinese)}개")
print(f"* 총 unique 중국 제조 공장 수: {unique_factory_names}개")
