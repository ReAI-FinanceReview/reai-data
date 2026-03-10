"""App Store 크롤러 클래스 (Issue #19: Batch DLQ)

Crawl 단계: API → Parquet 쓰기 + ingestion_batch PENDING 등록
Load 단계(BatchLoader)에서 Parquet → ReviewMasterIndex 적재
"""

import requests
import json
import os
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional
from uuid import UUID

from .base_crawler import BaseCrawler
from .exceptions import ParquetWriteError
from src.utils.db_connector import DatabaseConnector
from src.models.base import Base
from src.models.enums import PlatformType
from src.schemas.parquet.app_review import AppReviewSchema


class AppStoreCrawler(BaseCrawler):
    """App Store 리뷰 크롤러 (Batch DLQ Architecture)

    crawl 단계에서 Parquet 파일을 생성하고 ingestion_batch PENDING 레코드를 등록합니다.
    ReviewMasterIndex 생성은 load 단계(BatchLoader)에서 처리합니다.
    """

    def __init__(self, config_path: str = None):
        super().__init__(config_path)

        # App Store 특화 설정
        self.country = self.config.get('appstore', {}).get('country', 'kr')
        self.pages_to_crawl = self.config.get('appstore', {}).get('pages_to_crawl', 10)
        self.max_reviews_per_app = self.config.get('appstore', {}).get('max_reviews_per_app', 500)

        # 앱 ID 파일 경로
        self.app_ids_file = self.config.get('app_ids', {}).get('appstore', 'config/app_ids/appstore_app_ids.txt')

        # 데이터베이스 커넥터 초기화
        self.db_connector = DatabaseConnector(config_path or 'config/crawler_config.yml')

        self.enable_parquet = os.getenv('ENABLE_PARQUET_WRITE', 'true').lower() == 'true'

    def crawl_reviews(self, app_id: str) -> List[Dict[str, Any]]:
        """리뷰 크롤링 (추상 메서드 구현)"""
        _, reviews = self.get_app_store_reviews_and_appname(app_id)
        return reviews

    def _get_platform_type(self) -> PlatformType:
        """Get platform type for AppStore."""
        return PlatformType.APPSTORE

    def _extract_platform_review_id(self, review_data: Dict[str, Any]) -> str:
        """Extract review ID from App Store review data."""
        return review_data.get('id', {}).get('label', '')

    def _parse_reviewed_at(self, review_data: Dict[str, Any]) -> datetime:
        """Parse reviewed_at timestamp from App Store review data."""
        reviewed_at = None
        if 'updated' in review_data and 'label' in review_data['updated']:
            try:
                reviewed_at = datetime.fromisoformat(
                    review_data['updated']['label'].replace('Z', '+00:00')
                )
            except (ValueError, TypeError):
                pass
        if not reviewed_at:
            reviewed_at = datetime.now(timezone.utc)
        return reviewed_at

    def get_app_store_reviews_and_appname(
        self,
        app_id: str,
        country: str = None,
        pages: int = None
    ) -> Tuple[Optional[str], List[Dict[str, Any]]]:
        """지정된 앱 ID와 국가 코드로 App Store 리뷰와 앱 이름을 가져옵니다."""
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

    def _build_parquet_records(
        self,
        reviews_data: List[Dict[str, Any]],
        review_id_map: Dict[str, UUID],
        reviewed_at_cache: Dict[str, datetime],
        app
    ) -> List[AppReviewSchema]:
        """App Store 전용 Parquet 레코드 빌더."""
        parquet_records = []
        for review_data in reviews_data:
            platform_review_id = self._extract_platform_review_id(review_data)
            if not platform_review_id or platform_review_id not in review_id_map:
                continue

            review_text = review_data.get('content', {}).get('label', '')
            if not review_text:
                continue

            parquet_records.append(AppReviewSchema(
                review_id=str(review_id_map[platform_review_id]),
                app_id=str(app.app_id),
                platform_type='APPSTORE',
                platform_review_id=platform_review_id,
                reviewer_name=review_data.get('author', {}).get('name', {}).get('label'),
                review_text=review_text,
                rating=max(1, min(5, int(review_data.get('im:rating', {}).get('label', 1) or 1))),
                reviewed_at=reviewed_at_cache[platform_review_id],
                is_reply=False,
                reply_comment=None
            ))
        return parquet_records

    def run(self) -> None:
        """크롤러 실행: 모든 앱 순회 후 하루치 단일 파일을 MinIO에 업로드"""
        self.logger.info("App Store 크롤러 실행 시작")

        self.db_connector.create_tables(Base)

        app_ids = self.read_app_ids(self.app_ids_file)
        if not app_ids:
            raise ValueError(f"앱 ID 파일에서 유효한 ID를 찾을 수 없습니다: {self.app_ids_file}")

        self.logger.info(f"총 {len(app_ids)}개 앱의 리뷰를 크롤링합니다.")

        all_records = []
        successful_apps = 0

        for i, app_id in enumerate(app_ids, 1):
            self.logger.info(f"[{i}/{len(app_ids)}] 앱 ID: {app_id} 크롤링 시작...")

            try:
                app_name, reviews_data = self.get_app_store_reviews_and_appname(app_id)

                if not app_name:
                    self.logger.warning(f"앱 ID {app_id}의 이름을 찾을 수 없습니다. 건너뜁니다.")
                    continue

                if not reviews_data:
                    self.logger.warning(f"앱 ID {app_id}: 수집된 리뷰 없음")
                    continue

                records = self.collect_app_records(
                    app_id, app_name, reviews_data, self._build_parquet_records
                )

                if records:
                    self.logger.info(f"앱 ID {app_id}: {len(records)}개 신규 리뷰 수집")
                    all_records.extend(records)
                    successful_apps += 1
                else:
                    self.logger.info(f"앱 ID {app_id}: 새로운 리뷰 없음")

            except Exception as e:
                self.logger.error(f"앱 ID {app_id} 크롤링 실패: {e}")
                continue

            if i < len(app_ids):
                self.wait_between_requests()

        self.logger.info(f"크롤링 완료 - 성공: {successful_apps}/{len(app_ids)}개 앱, 총 {len(all_records)}개 리뷰")

        if all_records:
            try:
                _, count, s3_key = self.save_daily_batch(all_records, self._get_platform_type())
                if s3_key:
                    self.logger.info(f"MinIO 업로드 완료: {count}개 리뷰 → {s3_key}")
                else:
                    self.logger.info("Parquet 쓰기 비활성화 (ENABLE_PARQUET_WRITE=false) — 업로드 건너뜀")
            except ParquetWriteError as e:
                self.logger.error(f"MinIO 업로드 실패: {e}")
                raise
        else:
            self.logger.info("업로드할 신규 리뷰 없음")
