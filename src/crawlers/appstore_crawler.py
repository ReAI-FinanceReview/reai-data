"""App Store 크롤러 클래스 (Issue #19: Batch DLQ)

Crawl 단계: API → Parquet 쓰기 + ingestion_batch PENDING 등록
Load 단계(BatchLoader)에서 Parquet → ReviewMasterIndex 적재
"""

from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional
from uuid import UUID

from .appstore_rss import FeedRead, read_feed, read_outcome
from .base_crawler import BaseCrawler
from .crawl_outcome import (
    OUTCOME_ERROR,
    OUTCOME_FEED_EMPTY,
    OUTCOME_OK,
    OUTCOME_SKIPPED_AFTER_EMPTY_FEEDS,
    AppCrawlResult,
    StoreCrawlSummary,
)
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
        # RSS 읽기 설정: app_version 백필과 같은 키를 읽으며 기본값이 없다
        self.reviews_rss_url = self.config['appstore']['reviews_rss_url']
        self.rss_page_size = self.config['appstore']['rss_page_size']
        self.max_consecutive_empty_feeds = self.config['appstore']['max_consecutive_empty_feeds']

        # 앱 ID 파일 경로
        self.app_ids_file = self.config.get('app_ids', {}).get('appstore', 'config/app_ids/appstore_app_ids.txt')

        # 데이터베이스 커넥터 초기화
        self.db_connector = DatabaseConnector(config_path or 'config/crawler_config.yml')


    def crawl_reviews(self, app_id: str) -> List[Dict[str, Any]]:
        """리뷰 크롤링 (추상 메서드 구현)"""
        _, reviews = self.get_app_store_reviews_and_appname(app_id)
        return reviews

    def _get_platform_type(self) -> PlatformType:
        """Get platform type for AppStore."""
        return PlatformType.APPSTORE

    @staticmethod
    def _extract_platform_review_id(review_data: Dict[str, Any]) -> str:
        """Extract review ID from App Store review data."""
        return review_data.get('id', {}).get('label', '')

    @staticmethod
    def _extract_app_version(review_data: Dict[str, Any]) -> Optional[str]:
        """Extract the app version the review was written against (RSS `im:version.label`)."""
        return review_data.get('im:version', {}).get('label')

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

    def read_reviews(self, app_id: str, country: Optional[str] = None, pages: Optional[int] = None) -> FeedRead:
        """앱 하나의 RSS를 appstore_rss.read_feed로 읽는다. 빈 쪽에서 멈추지 않고, 리뷰가
        max_reviews_per_app건 모이면 멈춘다."""
        return read_feed(
            app_id,
            url_template=self.reviews_rss_url,
            country=country or self.country,
            pages=pages or self.pages_to_crawl,
            timeout=self.timeout,
            delay=self.delay,
            on_page=lambda read, _entries: read.fetched >= self.max_reviews_per_app,
        )

    def get_app_store_reviews_and_appname(
        self,
        app_id: str,
        country: str = None,
        pages: int = None
    ) -> Tuple[Optional[str], List[Dict[str, Any]]]:
        """지정된 앱 ID와 국가 코드로 App Store 리뷰와 앱 이름을 가져옵니다.

        앱 이름은 RSS 앱 메타 항목의 im:name이고, 메타 항목 없이 리뷰만 왔으면 app_{app_id}다.
        리뷰가 하나도 없으면 None이다.
        """
        read = self.read_reviews(app_id, country, pages)
        if read.error:
            self.logger.error(f"앱 ID {app_id}: HTTP 요청 중 에러 발생 - {read.error}")
        reviews = read.entries[:self.max_reviews_per_app]
        if read.app_name:
            app_name = read.app_name
        else:
            app_name = f'app_{app_id}' if reviews else None
        return app_name, reviews

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
                reply_comment=None,
                app_version=self._extract_app_version(review_data)
            ))
        return parquet_records

    def _crawl_app(self, app_id: str, all_records: List[AppReviewSchema]) -> AppCrawlResult:
        """앱 하나를 읽고 신규 레코드를 all_records에 더한 뒤, 그 앱의 결과를 돌려준다.

        읽은 뒤 레코드를 만드는 중의 예외(DB)는 읽은 건수와 함께 error로 남긴다. 읽는 중의
        예외는 호출자(run)가 error로 남긴다.
        """
        read = self.read_reviews(app_id)
        reviews_data = read.entries[:self.max_reviews_per_app]
        outcome = read_outcome(read, self.rss_page_size)
        detail = None
        if outcome == OUTCOME_ERROR:
            detail = read.error
        elif outcome != OUTCOME_OK:
            detail = f'쪽별 리뷰 수 {read.page_counts}'
        self.logger.info(
            f"앱 ID {app_id}: 리뷰 {len(reviews_data)}건, 읽은 쪽 {len(read.page_counts)}, "
            f"빈 쪽 {read.empty_pages}, 결과 {outcome}"
        )
        new = 0
        if reviews_data:
            app_name = read.app_name or f'app_{app_id}'
            try:
                records = self.collect_app_records(
                    app_id, app_name, reviews_data, self._build_parquet_records
                )
            except Exception as e:
                self.logger.exception(f"앱 ID {app_id} 신규 리뷰 확인 실패: {e}")
                return AppCrawlResult(app_id, OUTCOME_ERROR, fetched=len(reviews_data), detail=repr(e))
            all_records.extend(records)
            new = len(records)
            self.logger.info(f"앱 ID {app_id}: 신규 리뷰 {new}건")
        return AppCrawlResult(app_id, outcome, fetched=len(reviews_data), new=new, detail=detail)

    def run(self) -> StoreCrawlSummary:
        """크롤러 실행: 모든 앱 순회 후 하루치 단일 파일을 MinIO에 업로드하고 결과를 돌려준다.

        앱 하나가 비거나(feed_empty) 중간 쪽이 빠지거나(gap) 실패한(error) 것은 경고다. 리뷰를
        하나도 못 읽은 앱(빈 피드, 또는 첫 리뷰 전에 실패한 요청)이 appstore.max_consecutive_empty_feeds
        개 연달아 나오면 RSS 장애로 보고 남은 앱은 조회하지 않으며(skipped_after_empty_feeds),
        그때까지 모은 리뷰는 올린 뒤 ``failure``를 채워 돌려준다.
        """
        self.logger.info("App Store 크롤러 실행 시작")

        self.db_connector.create_tables(Base)

        app_ids = self.read_app_ids(self.app_ids_file)
        if not app_ids:
            raise ValueError(f"앱 ID 파일에서 유효한 ID를 찾을 수 없습니다: {self.app_ids_file}")

        self.logger.info(f"총 {len(app_ids)}개 앱의 리뷰를 크롤링합니다.")

        summary = StoreCrawlSummary(store='appstore')
        all_records = []
        nothing_read_in_a_row = 0

        for i, app_id in enumerate(app_ids, 1):
            if nothing_read_in_a_row >= self.max_consecutive_empty_feeds:
                summary.apps.append(AppCrawlResult(app_id, OUTCOME_SKIPPED_AFTER_EMPTY_FEEDS))
                continue

            self.logger.info(f"[{i}/{len(app_ids)}] 앱 ID: {app_id} 크롤링 시작...")
            try:
                result = self._crawl_app(app_id, all_records)
            except Exception as e:  # 읽는 중의 예외: 리뷰를 하나도 못 읽은 앱으로 센다
                self.logger.exception(f"앱 ID {app_id} 크롤링 실패: {e}")
                result = AppCrawlResult(app_id, OUTCOME_ERROR, detail=repr(e))
            summary.apps.append(result)

            nothing_read = result.fetched == 0 and result.outcome in (OUTCOME_FEED_EMPTY, OUTCOME_ERROR)
            nothing_read_in_a_row = nothing_read_in_a_row + 1 if nothing_read else 0

            if i < len(app_ids):
                self.wait_between_requests()

        if nothing_read_in_a_row >= self.max_consecutive_empty_feeds:
            skipped = sum(1 for app in summary.apps if app.outcome == OUTCOME_SKIPPED_AFTER_EMPTY_FEEDS)
            summary.failure = (
                f"App Store RSS: 리뷰를 하나도 못 읽은 앱이 {nothing_read_in_a_row}개 연달아 나와 "
                f"장애로 보고 남은 {skipped}개 앱은 조회하지 않음"
            )

        for app in summary.warnings:
            self.logger.warning(
                f"앱 ID {app.app_id}: {app.outcome}" + (f" ({app.detail})" if app.detail else "")
            )
        self.logger.info(
            f"크롤링 완료 - 앱 {len(app_ids)}개 중 경고 {len(summary.warnings)}개, "
            f"신규 리뷰 {len(all_records)}건"
        )

        if all_records:
            try:
                _, count, s3_key = self.save_daily_batch(all_records, self._get_platform_type())
                summary.storage_path = s3_key
                if s3_key:
                    self.logger.info(f"MinIO 업로드 완료: {count}개 리뷰 → {s3_key}")
                else:
                    self.logger.info("Parquet 쓰기 비활성화 (ENABLE_PARQUET_WRITE=false) — 업로드 건너뜀")
            except ParquetWriteError as e:
                self.logger.error(f"MinIO 업로드 실패: {e}")
                raise
        else:
            self.logger.info("업로드할 신규 리뷰 없음")

        if summary.failure:
            self.logger.error(f"❌ {summary.failure}")
        return summary
