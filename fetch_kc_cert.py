import asyncio
import aiohttp
import json
from datetime import datetime, timedelta

API_URL = "http://www.safetykorea.kr/openapi/api/cert/certificationList.json"
AUTH_KEY = "a5aa605a-1f01-4acd-b08d-21d425f8dc5a"


async def fetch_cert_by_date(session, date_str):
    headers = {"AuthKey": AUTH_KEY}
    params = {"conditionKey": "certDate", "conditionValue": date_str}
    try:
        async with session.get(API_URL, headers=headers, params=params) as response:
            if response.status == 200:
                result = await response.json()
                if result.get("resultCode") == "2000":
                    return result.get("resultData", [])
                else:
                    print(f"[{date_str}] API 오류: {result.get('resultMsg')}")
            else:
                print(f"[{date_str}] HTTP 오류: {response.status}")
    except Exception as e:
        print(f"[{date_str}] 예외 발생: {e}")
    return []


async def fetch_certifications_for_year(year):
    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 12, 31)
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)

    data_year = []
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_cert_by_date(session, date_str) for date_str in dates]
        results = await asyncio.gather(*tasks)
        for date_str, daily_data in zip(dates, results):
            print(f"{date_str}에 {len(daily_data)}건의 데이터 수집됨")
            data_year.extend(daily_data)
    return data_year


async def main():
    current_year = datetime.now().year
    all_years_data = {}
    for year in range(2000, current_year + 1):
        print(f"\n==== {year}년 데이터 수집 시작 ====")
        data_year = await fetch_certifications_for_year(year)
        all_years_data[year] = data_year
        file_name = f"certifications_{year}.json"
        with open(file_name, "w", encoding="utf-8") as f:
            json.dump(data_year, f, ensure_ascii=False, indent=4)
        print(f"{year}년: 총 {len(data_year)} 건의 데이터 저장됨. (파일명: {file_name})")

    with open("certifications_all_years.json", "w", encoding="utf-8") as f:
        json.dump(all_years_data, f, ensure_ascii=False, indent=4)
    print("\n모든 연도 데이터 저장 완료.")


if __name__ == "__main__":
    asyncio.run(main())
