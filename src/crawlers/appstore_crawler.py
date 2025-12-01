"""
App Store 크롤러 클래스
"""
import requests
import json
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional

from .base_crawler import BaseCrawler
from src.utils.db_connector import DatabaseConnector
from src.models.base import Base
from src.models.app import App
from src.models.review import Review


class AppStoreCrawler(BaseCrawler):
    """App Store 리뷰 크롤러"""

    def __init__(self, config_path: str = None):
        """
        Initialize the AppStoreCrawler, load App Store-specific configuration, and initialize the database connector.
        
        Parameters:
            config_path (str, optional): Path to the crawler configuration file used to read App Store settings and database configuration. If omitted, the superclass or default configuration is used.
        
        Attributes set:
            country (str): Country code for App Store requests (default 'kr').
            pages_to_crawl (int): Number of pages to fetch per app (default 10).
            max_reviews_per_app (int): Maximum number of reviews to collect per app (default 500).
            app_ids_file (str): File path that contains App Store app IDs.
            db_connector (DatabaseConnector): Connector used for database access and session management.
        """
        super().__init__(config_path)

        # App Store 특화 설정
        self.country = self.config.get('appstore', {}).get('country', 'kr')
        self.pages_to_crawl = self.config.get('appstore', {}).get('pages_to_crawl', 10)
        self.max_reviews_per_app = self.config.get('appstore', {}).get('max_reviews_per_app', 500)

        # 앱 ID 파일 경로
        self.app_ids_file = self.config.get('app_ids', {}).get('appstore', 'config/app_ids/appstore_app_ids.txt')

        # 데이터베이스 커넥터 초기화
        self.db_connector = DatabaseConnector(config_path or 'config/crawler_config.yml')

    def crawl_reviews(self, app_id: str) -> List[Dict[str, Any]]:
        """
        Fetches reviews for the specified App Store application identifier.
        
        Parameters:
            app_id (str): The App Store application identifier (e.g., numeric app ID).
        
        Returns:
            List[Dict[str, Any]]: A list of raw review entries obtained from the App Store feed; each entry is a dictionary matching the feed's JSON structure.
        """
        _, reviews = self.get_app_store_reviews_and_appname(app_id)
        return reviews

    def get_app_store_reviews_and_appname(self, app_id: str, country: str = None, pages: int = None) -> Tuple[Optional[str], List[Dict[str, Any]]]:
        """
        Fetches App Store reviews for the given app ID across multiple RSS pages and returns the detected app name and collected review entries.
        
        This method pages through Apple's RSS customer reviews feed (up to `pages`), normalizes entries, stops when no more entries are found or when `self.max_reviews_per_app` is reached, and returns the app name detected from the first page together with the raw review entry dictionaries.
        
        Parameters:
            app_id (str): The App Store application ID to crawl.
            country (str, optional): Two-letter country code to use for the feed; defaults to the crawler's configured country.
            pages (int, optional): Maximum number of pages to request; defaults to the crawler's configured pages_to_crawl.
        
        Returns:
            tuple:
                app_name (Optional[str]): The app name extracted from the first page, or None if not found.
                reviews (List[Dict[str, Any]]): Collected raw review entry dictionaries (truncated to `self.max_reviews_per_app` if necessary).
        """
        if country is None:
            country = self.country
        if pages is None:
            pages = self.pages_to_crawl
            
        all_reviews = []
        app_name = None
        
        self.logger.info(f"앱 ID {app_id}의 리뷰 크롤링 시작 (최대 {pages}페이지)")
        
        for page in range(1, pages + 1):
            url = f"https://itunes.apple.com/{country}/rss/customerreviews/page={page}/id={app_id}/sortby=mostRecent/json"
            
            try:
                response = requests.get(url, timeout=self.timeout)
                response.raise_for_status()
                data = response.json()
                
                entries = data.get('feed', {}).get('entry')
                if not entries:
                    self.logger.info(f"{page} 페이지에서 더 이상 리뷰를 찾을 수 없어 중단합니다.")
                    break
                
                if isinstance(entries, dict):
                    entries = [entries]
                elif not isinstance(entries, list):
                    self.logger.warning(f"{page} 페이지에서 유효하지 않은 데이터 구조입니다.")
                    break
                
                if page == 1:
                    if len(entries) > 0 and 'im:name' in entries[0]:
                        app_name = entries[0].get('im:name', {}).get('label', f'app_{app_id}')
                        self.logger.info(f"앱 이름: {app_name}")
                    else:
                        app_name = f'app_{app_id}'
                        for entry in entries:
                            all_reviews.append(entry)
                        self.logger.info(f"{page} 페이지의 리뷰를 성공적으로 가져왔습니다. (리뷰 수: {len(entries)})")
                        continue
                
                for entry in entries[1:]:
                    all_reviews.append(entry)
                
                review_count = len(entries) - 1 if page == 1 else len(entries)
                self.logger.info(f"{page} 페이지의 리뷰를 성공적으로 가져왔습니다. (리뷰 수: {review_count})")
                
                if len(all_reviews) >= self.max_reviews_per_app:
                    self.logger.info(f"최대 리뷰 수({self.max_reviews_per_app})에 도달하여 중단합니다.")
                    all_reviews = all_reviews[:self.max_reviews_per_app]
                    break
                    
            except requests.exceptions.RequestException as e:
                self.logger.error(f"HTTP 요청 중 에러 발생: {e}")
                break
            except json.JSONDecodeError:
                self.logger.error("JSON 파싱 중 에러 발생. 응답이 올바른 JSON 형식이 아닙니다.")
                break
            except Exception as e:
                self.logger.error(f"앱 ID {app_id}, 페이지 {page} 처리 중 예상치 못한 오류 발생: {e}")
                break
            
            if page < pages:
                self.wait_between_requests()
        
        self.logger.info(f"앱 ID {app_id}: 총 {len(all_reviews)}개 리뷰 수집 완료")
        return app_name, all_reviews

    def run(self) -> None:
        """
        Run the App Store crawler to fetch reviews for configured app IDs and persist new reviews to the database.
        
        Reads app IDs from the configured file, ensures database tables exist, then iterates each app ID: fetches the app name and reviews, updates the App's updated_at timestamp, inserts new Review records while skipping duplicates and entries missing required fields, commits successful changes, and logs progress and errors. Waits between requests when crawling multiple apps and closes the session when finished.
        
        Raises:
            ValueError: If no valid app IDs are found in the configured app IDs file.
        """
        self.logger.info("App Store 크롤러 실행 시작")
        
        self.db_connector.create_tables(Base)
        session = self.db_connector.get_session()
        
        app_ids = self.read_app_ids(self.app_ids_file)
        if not app_ids:
            raise ValueError(f"앱 ID 파일에서 유효한 ID를 찾을 수 없습니다: {self.app_ids_file}")
        
        self.logger.info(f"총 {len(app_ids)}개 앱의 리뷰를 크롤링합니다.")
        
        successful_apps = 0
        total_reviews_added = 0
        
        for i, app_id in enumerate(app_ids, 1):
            self.logger.info(f"[{i}/{len(app_ids)}] 앱 ID: {app_id} 크롤링 시작...")

            try:
                app_name, reviews_data = self.get_app_store_reviews_and_appname(app_id)

                if not app_name:
                    self.logger.warning(f"앱 ID {app_id}의 이름을 찾을 수 없습니다. 건너뜁니다.")
                    continue

                # App 정보 조회 (appstore_id로 조회)
                app = session.query(App).filter_by(appstore_id=app_id).first()
                if not app:
                    self.logger.warning(f"앱 ID {app_id} ({app_name})가 apps 테이블에 없습니다. 스킵합니다.")
                    continue
                else:
                    app.updated_at = datetime.now()

                if not reviews_data:
                    self.logger.warning(f"앱 ID {app_id}: 수집된 리뷰 없음")
                    session.commit()
                    continue

                reviews_added_for_app = 0
                for entry in reviews_data:
                    platform_review_id = entry.get('id', {}).get('label', '')
                    if not platform_review_id:
                        continue

                    # 중복 리뷰 확인
                    existing_review = session.query(Review).filter_by(
                        platform_review_id=platform_review_id,
                        app_id=app.id,
                        platform='APPSTORE'
                    ).first()
                    if existing_review:
                        continue

                    # reviewed_at 처리
                    reviewed_at = None
                    if 'updated' in entry and 'label' in entry['updated']:
                        try:
                            reviewed_at = datetime.fromisoformat(entry['updated']['label'].replace('Z', '+00:00'))
                        except (ValueError, TypeError):
                            pass
                    if not reviewed_at:
                        reviewed_at = datetime.now()

                    # review_text 처리
                    review_text = entry.get('content', {}).get('label', '')
                    if not review_text:
                        continue  # review_text는 NOT NULL

                    review = Review(
                        app_id=app.id,
                        platform='APPSTORE',
                        country_code='kr',
                        platform_review_id=platform_review_id,
                        reviewer_name=entry.get('author', {}).get('name', {}).get('label'),
                        review_text=review_text,
                        rating=int(entry.get('im:rating', {}).get('label', 0)),
                        app_version=entry.get('im:version', {}).get('label'),
                        reviewed_at=reviewed_at
                    )
                    session.add(review)
                    reviews_added_for_app += 1

                if reviews_added_for_app > 0:
                    self.logger.info(f"앱 ID {app_id}: {reviews_added_for_app}개의 새로운 리뷰 추가")
                    total_reviews_added += reviews_added_for_app
                else:
                    self.logger.info(f"앱 ID {app_id}: 새로운 리뷰 없음")

                successful_apps += 1
                session.commit()

            except Exception as e:
                self.logger.error(f"앱 ID {app_id} 크롤링 실패: {e}")
                session.rollback()
                continue
            
            if i < len(app_ids):
                self.wait_between_requests()
        
        session.close()
        self.logger.info(f"크롤링 완료 - 성공: {successful_apps}/{len(app_ids)}개 앱")
        self.logger.info(f"총 {total_reviews_added}개 리뷰가 데이터베이스에 추가되었습니다.")