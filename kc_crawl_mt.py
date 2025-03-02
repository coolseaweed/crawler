from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import json
import time
import argparse
import threading
from threading import Thread, Lock
import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime

HEADLESS = False
save_lock = Lock()  # 파일 저장을 위한 쓰레드 락
SLEEP_TIME = 2


def setup_logger(index):
    """각 쓰레드별 로거를 설정합니다."""
    logger = logging.getLogger(f"thread_{index}")

    # 이미 핸들러가 설정되어 있다면 추가 설정하지 않음
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    # 쓰레드별 로그 디렉토리 생성
    thread_log_dir = f"logs/Thread_{index}"
    os.makedirs(thread_log_dir, exist_ok=True)

    # 현재 날짜로 로그 파일명 생성 (YYMMDD 형식)
    current_date = datetime.now().strftime("%y%m%d")
    log_filename = f"{current_date}.log"

    # 파일 핸들러 설정
    file_handler = RotatingFileHandler(
        os.path.join(thread_log_dir, log_filename), maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"  # 10MB
    )

    # 포맷터 설정
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)

    # 스트림 핸들러 설정 (콘솔 출력용)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger


class SafetyKoreaCrawler:
    def __init__(self, index, base_dir="output", headless=False):
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--window-size=1920,1080")

        self.driver = webdriver.Chrome(options=chrome_options)
        self.crawled_data = []
        self.existing_cert_numbers = set()
        self.output_path = f"{base_dir}/{index}.json"  # 인덱스.json 형식으로 저장
        self.index = index
        self.logger = setup_logger(index)

    def load_existing_data(self):
        """기존 데이터 파일을 로드합니다."""
        try:
            with save_lock:  # 파일 읽기에도 락 사용
                with open(self.output_path, "r", encoding="utf-8") as f:
                    self.crawled_data = json.load(f)
            self.logger.info(f"기존 데이터 {len(self.crawled_data)}개 로드 완료")
            self.existing_cert_numbers = {
                item["인증정보"]["인증번호"].lower()
                for item in self.crawled_data
                if "인증정보" in item and "인증번호" in item["인증정보"]
            }
        except FileNotFoundError:
            self.logger.info("새로운 데이터 파일을 생성합니다.")

    def save_data(self):
        """수집된 데이터를 파일로 실시간으로 저장합니다."""
        try:
            with save_lock:  # 파일 쓰기 시 락 사용
                with open(self.output_path, "w", encoding="utf-8") as f:
                    json.dump(self.crawled_data, f, ensure_ascii=False, indent=4)
            self.logger.info(f"{len(self.crawled_data)}개의 데이터 저장 완료")
        except Exception as e:
            self.logger.error(f"데이터 저장 중 오류 발생: {e}")
            self._save_backup()

    def _save_backup(self):
        """백업 파일 저장을 시도합니다."""
        try:
            with save_lock:  # 백업 파일 저장 시에도 락 사용
                with open(f"backup_{self.output_path}", "w", encoding="utf-8") as f:
                    json.dump(self.crawled_data, f, ensure_ascii=False, indent=4)
            self.logger.info("데이터가 backup 파일로 저장되었습니다.")
        except:
            self.logger.error("데이터 저장에 완전히 실패했습니다.")

    def wait_for_element(self, by, value, wait_type="presence", timeout=10):
        """요소가 나타날 때까지 대기합니다."""
        wait = WebDriverWait(self.driver, timeout)
        if wait_type == "presence":
            return wait.until(EC.presence_of_element_located((by, value)))
        elif wait_type == "clickable":
            return wait.until(EC.element_to_be_clickable((by, value)))
        elif wait_type == "invisible":
            return wait.until(EC.invisibility_of_element_located((by, value)))
        elif wait_type == "all_present":
            return wait.until(EC.presence_of_all_elements_located((by, value)))

    def parse_detail_page(self, html_content):
        """상세 페이지의 데이터를 파싱합니다."""
        soup = BeautifulSoup(html_content, "html.parser")
        return {
            "인증정보": self._parse_key_value_table(soup, "인증정보 상세"),
            "제품정보": self._parse_key_value_table(soup, "제품정보 상세"),
            "제조공장": self._parse_list_table(soup, "제조공장 상세", ["번호", "제조공장", "제조국"]),
            "연관 인증 번호": self._parse_list_table(soup, "연관 인증 번호 상세", ["번호", "인증번호", "인증상태"]),
        }

    def _parse_key_value_table(self, soup, caption_text):
        """키-값 테이블을 파싱합니다."""
        data = {}
        caption = soup.find("caption", string=lambda t: t and caption_text in t)
        if caption:
            table = caption.find_parent("table")
            for row in table.find_all("tr"):
                for th in row.find_all("th"):
                    key = th.get_text(strip=True)
                    td = th.find_next_sibling("td")
                    value = td.get_text(strip=True) if td else ""
                    data[key] = value
        return data

    def _parse_list_table(self, soup, caption_text, header_keys):
        """리스트 형태의 테이블을 파싱합니다."""
        items = []
        caption = soup.find("caption", string=lambda t: t and caption_text in t)
        if caption:
            table = caption.find_parent("table")
            for row in table.find_all("tr")[1:]:  # Skip header row
                cols = row.find_all(["th", "td"])
                if len(cols) >= len(header_keys):
                    item = {}
                    for idx, key in enumerate(header_keys):
                        value = (
                            cols[idx].find("a").get_text(strip=True)
                            if cols[idx].find("a")
                            else cols[idx].get_text(strip=True)
                        )
                        item[key] = value
                    items.append(item)
        return items

    def process_row(self, row_index):
        """각 행의 데이터를 처리하고 실시간으로 저장합니다."""
        try:
            rows = self.wait_for_element(By.CSS_SELECTOR, "table.tb_list tr[onclick]", "all_present")

            # row_index가 10 이상이면 mod 10으로 변환
            actual_index = row_index % 10

            # 행 개수 확인 및 인덱스 검증
            if actual_index >= len(rows):
                self.logger.error(f"Invalid row index: {actual_index}, Total rows: {len(rows)}")
                return

            row = rows[actual_index]
            cert_number = row.find_element(By.CSS_SELECTOR, "td:last-child").text.strip().lower()

            if cert_number in self.existing_cert_numbers:
                self.logger.info(f"Skip existing cert number: {cert_number}")
                return

            time.sleep(SLEEP_TIME)
            row.click()

            self.wait_for_element(By.CLASS_NAME, "contents_area")
            data = self.parse_detail_page(self.driver.page_source)

            if "인증정보" in data and "인증번호" in data["인증정보"]:
                cert_number = data["인증정보"]["인증번호"].lower()
                if cert_number not in self.existing_cert_numbers:
                    self.crawled_data.append(data)
                    self.existing_cert_numbers.add(cert_number)
                    self.logger.info(
                        f"Added new cert number: {cert_number} [{len(self.crawled_data)}] (actual_index: {actual_index})"
                    )
                    # 데이터가 추가될 때마다 저장
                    self.save_data()

            time.sleep(SLEEP_TIME)
            self.driver.back()
            self.wait_for_element(By.CLASS_NAME, "tb_list")
        except Exception as e:
            self.logger.error(f"Row processing error: {e}")
            raise

    def crawl_forward(self, index):
        """앞으로 이동하면서 크롤링을 실행합니다."""
        try:
            self.driver.get("https://www.safetykorea.kr/release/itemSearch")
            self.load_existing_data()
            next_button = self.wait_for_element(By.XPATH, "//a[@title='다음 페이지']", "clickable")
            next_button.click()

            while True:
                try:
                    self.process_row(index)
                except Exception as e:
                    self.logger.error(f"Row processing error: {e}")
                    continue

                try:
                    self.wait_for_element(By.ID, "loading", "invisible")
                    next_button = self.wait_for_element(By.XPATH, "//a[@title='다음 페이지']", "clickable")
                    next_button.click()
                    time.sleep(SLEEP_TIME)
                except Exception as e:
                    self.logger.error(f"Navigation error: {e}")
                    break

        except KeyboardInterrupt:
            self.logger.info("사용자에 의해 중단되었습니다.")
        except Exception as e:
            self.logger.error(f"예상치 못한 오류: {e}")
        finally:
            self.driver.quit()

    def crawl_backward(self, index):
        """뒤로 이동하면서 크롤링을 실행합니다."""
        try:
            self.driver.get("https://www.safetykorea.kr/release/itemSearch")
            self.load_existing_data()

            # 마지막 페이지로 이동
            last_page_button = self.wait_for_element(By.XPATH, "//a[@title='마지막 페이지']", "clickable")
            last_page_button.click()
            time.sleep(SLEEP_TIME)

            while True:
                try:
                    self.process_row(index)
                except Exception as e:
                    self.logger.error(f"Row processing error: {e}")
                    continue

                try:
                    self.wait_for_element(By.ID, "loading", "invisible")
                    prev_button = self.wait_for_element(By.XPATH, "//a[@title='이전 페이지']", "clickable")
                    prev_button.click()
                    time.sleep(SLEEP_TIME)
                except Exception as e:
                    self.logger.error(f"Navigation error: {e}")
                    break

        except KeyboardInterrupt:
            self.logger.info("사용자에 의해 중단되었습니다.")
        except Exception as e:
            self.logger.error(f"예상치 못한 오류: {e}")
        finally:
            self.driver.quit()


def run_crawler(index, direction="forward"):
    """각 쓰레드에서 실행될 크롤러 함수"""
    try:
        crawler = SafetyKoreaCrawler(index, headless=HEADLESS)
        crawler.logger.info(
            f"Start Crawling - output path: {crawler.output_path} (directioin: {direction}, idx: {index})"
        )

        if direction == "forward":
            crawler.crawl_forward(index)
        else:
            crawler.crawl_backward(index)

    except Exception as e:
        crawler.logger.error(f"오류 발생 - {e}")


def main():
    """멀티쓰레드로 크롤러를 실행합니다."""
    parser = argparse.ArgumentParser(description="Safety Korea 데이터 크롤러 (멀티쓰레드)")
    parser.add_argument("--threads", type=int, default=10, help="실행할 쓰레드 수 (기본값: 10)")

    args = parser.parse_args()

    if args.threads > 20:
        print("경고: 쓰레드 수는 최대 20개까지만 지원됩니다. 20개로 제한합니다.")
        args.threads = 20

    threads = []

    try:
        # 쓰레드 생성 및 시작
        for i in range(args.threads):
            # direction = "backward" if i // 10 == 1 else "forward"
            direction = "forward" if i // 10 == 1 else "backward"
            t = Thread(
                target=run_crawler,
                args=(
                    i,
                    direction,
                ),
            )
            t.start()
            threads.append(t)
            print(f"Thread {i} started (direction: {direction}, idx: {i})")

        # 모든 쓰레드 종료 대기
        for t in threads:
            t.join()

    except KeyboardInterrupt:
        print("\n사용자에 의해 중단되었습니다.")
        # 프로그램 종료
        print("프로그램이 종료되었습니다.")


if __name__ == "__main__":
    main()
