"""Play Store 크롤러 클래스 (Issue #19: Batch DLQ)

Crawl 단계: API → Parquet 쓰기 + ingestion_batch PENDING 등록
Load 단계(BatchLoader)에서 Parquet → ReviewMasterIndex 적재
"""

from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional
from uuid import UUID

from .base_crawler import BaseCrawler
from .crawl_outcome import (
    OUTCOME_ERROR,
    OUTCOME_FEED_EMPTY,
    OUTCOME_OK,
    AppCrawlResult,
    StoreCrawlSummary,
    playstore_failure,
)
from .exceptions import ParquetWriteError
from .playstore_reviews import PlayReviewsError, fetch_reviews_page, play_reviews_settings
from src.utils.db_connector import DatabaseConnector
from src.models.base import Base
from src.models.enums import PlatformType
from src.schemas.parquet.app_review import AppReviewSchema


class PlayStoreCrawler(BaseCrawler):
    """Play Store 리뷰 크롤러 (Batch DLQ Architecture)

    crawl 단계에서 Parquet 파일을 생성하고 ingestion_batch PENDING 레코드를 등록합니다.
    ReviewMasterIndex 생성은 load 단계(BatchLoader)에서 처리합니다.
    """

    def __init__(self, config_path: str = None):
        super().__init__(config_path)

        # Play Store 특화 설정
        self.reviews_per_app = self.config.get('playstore', {}).get('reviews_per_app', 100)
        # 리뷰 클라이언트 설정: app_version 백필과 같은 키를 읽으며 기본값이 없다
        self.play_client = play_reviews_settings(self.config)
        self.page_size = self.config['playstore']['page_size']

        # 앱 ID 파일 경로
        self.app_ids_file = self.config.get('app_ids', {}).get('playstore', 'config/app_ids/playstore_app_ids.txt')

        # 데이터베이스 커넥터 초기화
        self.db_connector = DatabaseConnector(config_path or 'config/crawler_config.yml')


    def crawl_reviews(self, app_id: str) -> List[Dict[str, Any]]:
        """리뷰 크롤링 (추상 메서드 구현)"""
        return self.get_playstore_reviews(app_id)

    def _get_platform_type(self) -> PlatformType:
        """Get platform type for PlayStore."""
        return PlatformType.PLAYSTORE

    @staticmethod
    def _extract_platform_review_id(review_data: Dict[str, Any]) -> str:
        """Extract reviewId from Play Store review data."""
        return review_data.get('reviewId', '')

    @staticmethod
    def _extract_app_version(review_data: Dict[str, Any]) -> Optional[str]:
        """Extract the app version the review was written against (`reviewCreatedVersion`)."""
        return review_data.get('reviewCreatedVersion')

    def _parse_reviewed_at(self, review_data: Dict[str, Any]) -> datetime:
        """Parse reviewed_at timestamp from Play Store review data."""
        reviewed_at = review_data.get('at')
        if not isinstance(reviewed_at, datetime):
            reviewed_at = datetime.now(timezone.utc)
        elif reviewed_at.tzinfo is None:
            reviewed_at = reviewed_at.replace(tzinfo=timezone.utc)
        return reviewed_at

    def get_playstore_reviews(self, app_id: str, count: int = None) -> List[Dict[str, Any]]:
        """최신 리뷰를 count건(기본 playstore.reviews_per_app)까지 가져옵니다.

        쪽은 _read_reviews가 읽습니다. 요청이 실패하면 PlayReviewsError를 그대로 올리고 앞서
        읽은 리뷰는 돌려주지 않습니다(빈 목록으로 바꾸지 않음). run()은 _read_reviews로 그
        리뷰까지 남깁니다.
        """
        reviews, error = self._read_reviews(app_id, count)
        if error is not None:
            raise error
        return reviews

    def _read_reviews(
        self, app_id: str, count: int = None
    ) -> Tuple[List[Dict[str, Any]], Optional[PlayReviewsError]]:
        """최신 리뷰를 count건(기본 playstore.reviews_per_app)까지 읽고 (읽은 리뷰, 오류)를 돌려줍니다.

        요청 1회에 playstore.page_size건까지 받고, 더 필요하면 continuation token으로 다음 쪽을
        요청합니다. 토큰 없는 쪽이 오면 거기서 멈춥니다. 요청이 실패하거나 리뷰 없이 토큰만 있는
        쪽이 오면(같은 쪽을 끝없이 요청하지 않도록) 거기서 멈추고, 그때까지 읽은 리뷰와
        PlayReviewsError를 돌려줍니다. 끝까지 읽으면 오류는 None입니다.
        """
        if count is None:
            count = self.reviews_per_app

        self.logger.info(f"Play Store 리뷰 수집 시작 - 앱 ID: {app_id}, 개수: {count}")
        result: List[Dict[str, Any]] = []
        token = None
        try:
            while len(result) < count:
                if token is not None:
                    self.wait_between_requests()
                page = fetch_reviews_page(
                    app_id, min(self.page_size, count - len(result)), self.play_client, token
                )
                if not page.reviews and page.next_token is not None:
                    raise PlayReviewsError(
                        f'a page without reviews came with a next-page token after {len(result)} reviews'
                    )
                result.extend(page.reviews)
                token = page.next_token
                if token is None:
                    break
        except PlayReviewsError as exc:
            return result, exc

        self.logger.info(f"앱 ID {app_id}: {len(result)}개 리뷰 수집 완료")
        return result, None

    def _build_parquet_records(
        self,
        reviews_data: List[Dict[str, Any]],
        review_id_map: Dict[str, UUID],
        reviewed_at_cache: Dict[str, datetime],
        app
    ) -> List[AppReviewSchema]:
        """Play Store 전용 Parquet 레코드 빌더."""
        parquet_records = []
        for review_data in reviews_data:
            platform_review_id = self._extract_platform_review_id(review_data)
            if not platform_review_id or platform_review_id not in review_id_map:
                continue

            review_text = review_data.get('content', '')
            if not review_text or not review_text.strip():
                continue

            try:
                rating = int(review_data.get('score') or 1)
            except (TypeError, ValueError):
                rating = 1
            if rating < 1:
                rating = 1
            elif rating > 5:
                rating = 5

            parquet_records.append(AppReviewSchema(
                review_id=str(review_id_map[platform_review_id]),
                app_id=str(app.app_id),
                platform_type='PLAYSTORE',
                platform_review_id=platform_review_id,
                reviewer_name=review_data.get('userName'),
                review_text=review_text,
                rating=rating,
                reviewed_at=reviewed_at_cache[platform_review_id],
                is_reply=False,
                reply_comment=review_data.get('replyContent'),
                app_version=self._extract_app_version(review_data)
            ))
        return parquet_records

    def run(self) -> StoreCrawlSummary:
        """크롤러 실행: 모든 앱 순회 후 하루치 단일 파일을 MinIO에 업로드하고 결과를 돌려준다.

        앱 하나가 리뷰 0건(feed_empty)이거나 실패한(error) 것은 경고다. 요청이 실패한 앱은
        error가 되고, 그 전 쪽에서 읽은 리뷰는 레코드로 만들어 남긴다(fetched는 읽은 건수, 첫
        요청이 실패하면 0). 모든 앱이 0건이면(playstore_failure) 모은 것이 없으니 ``failure``를
        채워 돌려준다.
        """
        self.logger.info("Play Store 크롤러 실행 시작")

        self.db_connector.create_tables(Base)

        app_ids = self.read_app_ids(self.app_ids_file)
        if not app_ids:
            raise ValueError(f"앱 ID 파일에서 유효한 ID를 찾을 수 없습니다: {self.app_ids_file}")

        self.logger.info(f"총 {len(app_ids)}개 앱의 리뷰를 크롤링합니다.")

        summary = StoreCrawlSummary(store='playstore')
        all_records = []

        for i, app_id in enumerate(app_ids, 1):
            self.logger.info(f"[{i}/{len(app_ids)}] 앱 ID: {app_id} 크롤링 시작...")

            reviews_data = []
            try:
                reviews_data, error = self._read_reviews(app_id)
                if error is not None:
                    self.logger.error(
                        f"앱 ID {app_id} 리뷰 요청 실패 (앞서 읽은 {len(reviews_data)}건은 남김): {error!r}"
                    )

                new = 0
                if reviews_data:
                    # 카탈로그(apps)에 행이 있으면 그 이름을 그대로 두고, 없을 때만 이 이름으로 만든다
                    records = self.collect_app_records(
                        app_id, f'app_{app_id}', reviews_data, self._build_parquet_records
                    )
                    all_records.extend(records)
                    new = len(records)
                    self.logger.info(f"앱 ID {app_id}: 리뷰 {len(reviews_data)}건, 신규 {new}건")

                if error is not None:
                    result = AppCrawlResult(
                        app_id, OUTCOME_ERROR, fetched=len(reviews_data), new=new, detail=repr(error)
                    )
                elif not reviews_data:
                    result = AppCrawlResult(app_id, OUTCOME_FEED_EMPTY)
                else:
                    result = AppCrawlResult(app_id, OUTCOME_OK, fetched=len(reviews_data), new=new)

            except Exception as e:
                self.logger.exception(f"앱 ID {app_id} 크롤링 실패: {e}")
                result = AppCrawlResult(app_id, OUTCOME_ERROR, fetched=len(reviews_data), detail=repr(e))
            summary.apps.append(result)

            if i < len(app_ids):
                self.wait_between_requests()

        summary.failure = playstore_failure(summary.apps)

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
