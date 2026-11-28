"""Play 스토어 리뷰를 batchexecute로 한 쪽씩 읽는 자체 클라이언트 (Issue #133)

평소 크롤(PlayStoreCrawler.get_playstore_reviews)과 app_version 백필(app_version_backfill.fetch_playstore_versions)이
같은 코드로 읽는다. google-play-scraper 1.2.7의 리뷰 조회(features/reviews.py)를 대신한다.

- 요청: ``playstore.reviews_url``(언어, 국가를 채운 batchexecute URL)에 ``f.req`` 폼을 POST한다.
  ``f.req``는 RPC id(``playstore.reviews_rpc_id``)와 조회 배열
  ``[null,[2,정렬,[개수,null,토큰],null,[null × 9]],["앱ID",7]]``이다. 정렬은 최신순만 쓴다.
- 응답: 앞의 ``)]}'``를 떼고 JSON으로 읽어 ``["wrb.fr", RPC id, payload, ...]`` 항목을 찾는다.
  payload(JSON 문자열)의 ``[0]``이 리뷰 목록, ``[-2][-1]``이 다음 쪽 토큰이다. 마지막 쪽은
  ``[-2]``가 null이다(2026-11-28 실측: 끝까지 넘긴 앱 5개 모두 덜 찬 쪽에 토큰 없음).
- 요청 한 번이 한 쪽이다. 연결 오류, 본문을 읽다 끊긴 응답, timeout, HTTP 429/5xx, payload 없이
  PlayGatewayError를 담은 응답은 ``playstore.max_retries``번까지 다시 요청한다. 요청이 실패하면
  (재시도 뒤에도 남은 위 오류, TLS 실패, 그 밖의 HTTP 상태, 모양이 깨진 응답) ``PlayReviewsError``를
  올린다. 피드의 끝(토큰 없음)이나 빈 목록과는 섞이지 않는다.
- TLS는 requests가 검증하고, 프로세스의 기본 TLS 컨텍스트는 건드리지 않는다.
"""

import json
import time
from dataclasses import dataclass
from datetime import datetime
from http import HTTPStatus
from typing import Any, Dict, List, Optional, Tuple

import requests

from src.utils.logger import get_logger

logger = get_logger(__name__)

# Fixed parts of the request and response format, as google-play-scraper 1.2.7 sends and reads them
# (constants/request.py `_Reviews`, utils/request.py, constants/regex.py `REVIEWS`).
_QUERY_KIND = 2  # first element of the review query
_SORT_NEWEST = 2  # google_play_scraper.Sort.NEWEST
_FILTER_SLOTS = 9  # [null, score filter, ..., device filter]; both filters stay null
_APP_ID_KIND = 7  # marker that follows the app id
_RPC_MODE = 'generic'
_RESPONSE_PREFIX = ")]}'"
_RESPONSE_ENTRY = 'wrb.fr'
GATEWAY_ERROR_MARKER = 'com.google.play.gateway.proto.PlayGatewayError'

# Positions inside one review item (google-play-scraper 1.2.7 `ElementSpecs.Review`, without
# `userImage` and the duplicate `appVersion`). A missing position reads as None, as it does there,
# except the fields in _REQUIRED_FIELDS. Positions the library does not map are not read.
_REVIEW_FIELDS = {
    'reviewId': (0,),
    'userName': (1, 0),
    'score': (2,),
    'content': (4,),
    'at': (5, 0),  # epoch seconds
    'thumbsUpCount': (6,),
    'replyContent': (7, 1),
    'repliedAt': (7, 2, 0),  # epoch seconds; the whole [7] is null when there is no reply
    'reviewCreatedVersion': (10,),
}
_TIMESTAMP_FIELDS = ('at', 'repliedAt')


class PlayReviewsError(Exception):
    """A Play reviews request failed; never the end of the feed, which is a page without a token."""


@dataclass(frozen=True)
class PlayReviewsSettings:
    """Client settings, read from crawler_config.yml by ``play_reviews_settings`` with no coded fallbacks."""

    url_template: str
    rpc_id: str
    language: str
    country: str
    timeout_seconds: float
    max_retries: int
    retry_backoff_seconds: float


def play_reviews_settings(config: Dict[str, Any]) -> PlayReviewsSettings:
    """Client settings from a parsed crawler_config.yml; a missing key raises ``KeyError``.

    The request timeout is ``global.timeout``, the one the App Store RSS requests use.
    """
    play = config['playstore']
    return PlayReviewsSettings(
        url_template=play['reviews_url'],
        rpc_id=play['reviews_rpc_id'],
        language=play['language'],
        country=play['country'],
        timeout_seconds=config['global']['timeout'],
        max_retries=play['max_retries'],
        retry_backoff_seconds=play['retry_backoff_seconds'],
    )


@dataclass
class PlayPage:
    """One page of reviews, newest first. ``next_token`` is None on the last page."""

    reviews: List[Dict[str, Any]]
    next_token: Optional[str]


def _compact_json(value: Any) -> str:
    return json.dumps(value, separators=(',', ':'))


def request_form(app_id: str, count: int, token: Optional[str], rpc_id: str) -> Dict[str, str]:
    """The ``f.req`` form for one page: the first page without ``token``, a later one with it."""
    paging = [count] if token is None else [count, None, token]
    query = [None, [_QUERY_KIND, _SORT_NEWEST, paging, None, [None] * _FILTER_SLOTS], [app_id, _APP_ID_KIND]]
    return {'f.req': _compact_json([[[rpc_id, _compact_json(query), None, _RPC_MODE]]])}


def _lookup(item: Any, path: Tuple[int, ...]) -> Any:
    value = item
    for index in path:
        try:
            value = value[index]
        except (IndexError, KeyError, TypeError):
            return None
    return value


_SCORES = range(1, 6)  # star ratings


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


# Fields every live review carried (2026-11-28: all 9,882 reviews on 36 captured pages), each with
# what it must be. A review without one means the positions moved, so parse_reviews_response
# raises instead of reading None.
_REQUIRED_FIELDS = {
    'reviewId': ('a non-empty str', lambda value: isinstance(value, str) and value != ''),
    'score': (
        f'an int from {_SCORES[0]} to {_SCORES[-1]}', lambda value: _is_int(value) and value in _SCORES
    ),
    'at': ('an int epoch', _is_int),
}


def review_from_item(item: Any) -> Dict[str, Any]:
    """One review dict with the keys and value types google-play-scraper 1.2.7 returned.

    ``at`` and ``repliedAt`` are naive local time (``datetime.fromtimestamp``), as they were there;
    callers rely on that for ``at`` (PlayStoreCrawler._parse_reviewed_at,
    app_version_backfill.scan_play_page).
    """
    review = {key: _lookup(item, path) for key, path in _REVIEW_FIELDS.items()}
    for key in _TIMESTAMP_FIELDS:
        try:
            review[key] = datetime.fromtimestamp(review[key])
        except (TypeError, ValueError, OverflowError, OSError):
            review[key] = None
    return review


def parse_reviews_response(text: str, rpc_id: str) -> PlayPage:
    """Read one batchexecute response body; a body of any other shape raises ``PlayReviewsError``."""
    if not text.startswith(_RESPONSE_PREFIX):
        raise PlayReviewsError(f'response does not start with {_RESPONSE_PREFIX!r}: {text[:80]!r}')
    try:
        envelope = json.loads(text[len(_RESPONSE_PREFIX):])
        entry = next(
            item for item in envelope
            if isinstance(item, list) and item[:2] == [_RESPONSE_ENTRY, rpc_id]
        )
    except (ValueError, TypeError, StopIteration, RecursionError) as exc:
        raise PlayReviewsError(f'no {_RESPONSE_ENTRY} {rpc_id} entry in the response: {exc!r}') from exc
    if len(entry) < 3 or not isinstance(entry[2], str):
        # Seen with a bad continuation token: ["wrb.fr", rpc_id, null, null, null, [3], "generic"].
        raise PlayReviewsError(f'{rpc_id} answered without a payload: {entry!r}')
    try:
        payload = json.loads(entry[2])
        items = payload[0]
        tail = payload[-2]
    except (ValueError, TypeError, IndexError, KeyError, RecursionError) as exc:
        raise PlayReviewsError(f'malformed {rpc_id} payload: {exc!r}') from exc
    if items is None:  # a page without reviews
        items = []
    if not isinstance(items, list):
        raise PlayReviewsError(f'malformed {rpc_id} payload: reviews {type(items).__name__}')
    if tail is not None and not isinstance(tail, list):
        raise PlayReviewsError(f'malformed {rpc_id} payload: token slot {type(tail).__name__}')
    token = tail[-1] if tail else None
    if not (token is None or isinstance(token, str)):
        raise PlayReviewsError(f'malformed {rpc_id} payload: token {type(token).__name__}')
    for index, item in enumerate(items):
        for key, (expected, valid) in _REQUIRED_FIELDS.items():
            value = _lookup(item, _REVIEW_FIELDS[key])
            if not valid(value):
                # The type only: once positions move, the value can be review text or a name.
                raise PlayReviewsError(
                    f'malformed {rpc_id} payload: item {index} {key} is {type(value).__name__}, '
                    f'expected {expected}'
                )
    return PlayPage([review_from_item(item) for item in items], token)


def _has_payload(text: str, rpc_id: str) -> bool:
    """Whether the body holds a ``wrb.fr`` entry for ``rpc_id`` with a string payload; a body that
    does not parse has none."""
    try:
        envelope = json.loads(text.removeprefix(_RESPONSE_PREFIX))
        return any(
            isinstance(item, list) and item[:2] == [_RESPONSE_ENTRY, rpc_id]
            and len(item) > 2 and isinstance(item[2], str)
            for item in envelope
        )
    except (ValueError, TypeError, RecursionError):
        return False


def _post(url: str, form: Dict[str, str], settings: PlayReviewsSettings) -> str:
    """POST with TLS verification and the timeout, retrying transient failures with linear backoff.

    Retried: connection errors, a connection dropped while the body is read, timeouts, HTTP 429
    and 5xx, and a body without a payload that carries PlayGatewayError. A TLS failure (certificate
    verification or handshake) and any other HTTP error status are raised at once.
    """
    attempts = settings.max_retries + 1
    failure: Optional[str] = None
    cause: Optional[BaseException] = None
    for attempt in range(attempts):
        if attempt:
            logger.warning(f"🤖 Play 리뷰 요청 재시도 {attempt}/{settings.max_retries}: {failure}")
            time.sleep(settings.retry_backoff_seconds * attempt)
        try:
            response = requests.post(url, data=form, timeout=settings.timeout_seconds, verify=True)
        except requests.exceptions.SSLError as exc:
            # A ConnectionError subclass, but a certificate or handshake failure is not transient.
            raise PlayReviewsError(f'TLS handshake or certificate verification failed: {exc!r}') from exc
        except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError) as exc:
            failure, cause = repr(exc), exc
            continue
        except requests.RequestException as exc:
            raise PlayReviewsError(f'request failed: {exc!r}') from exc
        status = response.status_code
        if status == HTTPStatus.TOO_MANY_REQUESTS or status >= HTTPStatus.INTERNAL_SERVER_ERROR:
            failure, cause = f'HTTP {status}', None
            continue
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise PlayReviewsError(f'HTTP {status}') from exc
        # No live PlayGatewayError answer has been captured, so its shape is unknown and the name is
        # matched anywhere in the body, as google-play-scraper 1.2.7 did, but only in a body without
        # a payload: a page's review text can contain the name too.
        if GATEWAY_ERROR_MARKER in response.text and not _has_payload(response.text, settings.rpc_id):
            failure, cause = 'PlayGatewayError', None
            continue
        return response.text
    raise PlayReviewsError(f'{attempts} attempts failed, last: {failure}') from cause


def fetch_reviews_page(
    app_id: str, count: int, settings: PlayReviewsSettings, token: Optional[str] = None
) -> PlayPage:
    """Request one page of up to ``count`` newest reviews: the first page, or the one ``token`` names.

    Raises ``PlayReviewsError`` when the request fails or the response cannot be read.
    """
    url = settings.url_template.format(language=settings.language, country=settings.country)
    text = _post(url, request_form(app_id, count, token, settings.rpc_id), settings)
    return parse_reviews_response(text, settings.rpc_id)
