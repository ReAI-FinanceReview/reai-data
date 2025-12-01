"""
임베딩 벡터 생성 파이프라인
전처리된 텍스트를 벡터로 변환하여 Silver Layer에 저장
"""
from datetime import datetime
from typing import Optional, List
import os
import time
from pathlib import Path

try:
    from openai import OpenAI
    from openai import APIError, RateLimitError
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    OpenAI = None
    APIError = Exception  # type: ignore
    RateLimitError = Exception  # type: ignore

try:
    from dotenv import load_dotenv
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False

from src.utils.logger import get_logger
from src.utils.db_connector import DatabaseConnector
from src.models.base import Base
from src.models.review_preprocessed import ReviewPreprocessed
from src.models.review_embedding import ReviewEmbedding
from src.models.llm_analysis_log import LLMAnalysisLog


class EmbeddingGenerator:
    """
    임베딩 벡터 생성 클래스
    
    OpenAI 임베딩 API(text-embedding-3-small 등)를 사용하여 텍스트를 벡터로 변환
    """

    def __init__(self, 
                 config_path: str = 'config/crawler_config.yml',
                 model_name: str = 'text-embedding-3-small',
                 base_url: Optional[str] = None):
        # .env 자동 로드 (배포 패키지 루트 기준)
        """
                 Create and configure an EmbeddingGenerator instance.
                 
                 Initializes environment variables (if available), logger, database connector, embedding metadata (version, model name, inferred vector dimension), retry policy, and attempts to initialize the OpenAI client.
                 
                 Parameters:
                     config_path (str): Path to the crawler configuration YAML used to configure the database connector.
                     model_name (str): Embedding model identifier to use for generating vectors.
                     base_url (Optional[str]): Optional base URL for the OpenAI API; if omitted, the value is taken from the OPENAI_BASE_URL environment variable.
                 """
                 if DOTENV_AVAILABLE:
            env_path = Path(__file__).resolve().parents[2] / ".env"
            if env_path.exists():
                load_dotenv(env_path)
        self.logger = get_logger(__name__)
        self.db_connector = DatabaseConnector(config_path)
        self.embedding_version = '1.0'
        self.model_name = model_name
        self.base_url = base_url or os.getenv("OPENAI_BASE_URL")
        self.api_key = os.getenv("OPENAI_API_KEY")
        self.client: Optional[OpenAI] = None
        self.vector_dimension = self._infer_dimension(model_name)
        self.max_retries = 3
        self.retry_backoff = 2.0

        # 클라이언트 초기화
        self._initialize_client()

    def _infer_dimension(self, model_name: str) -> int:
        """
        Determine the embedding vector dimension for a given model name.
        
        Supports "text-embedding-3-small" (1536) and "text-embedding-3-large" (3072). If the model name is not recognized, defaults to 1536.
        
        Parameters:
            model_name (str): Name of the embedding model.
        
        Returns:
            int: The vector dimension associated with the model.
        """
        dimension_map = {
            "text-embedding-3-small": 1536,
            "text-embedding-3-large": 3072,
        }
        return dimension_map.get(model_name, 1536)

    def _initialize_client(self):
        """
        Initialize the OpenAI client used for embedding requests.
        
        Sets self.client to an OpenAI client configured with the instance's api_key and base_url.
        If the OpenAI package is not available or the API key is missing, logs an error and leaves self.client unset (None).
        On initialization failure, clears self.client and logs the error.
        """
        if not OPENAI_AVAILABLE:
            self.logger.error("openai 패키지가 설치되지 않았습니다. `pip install openai` 후 다시 시도하세요.")
            return

        if not self.api_key:
            self.logger.error("OPENAI_API_KEY 환경변수가 설정되지 않았습니다.")
            return

        try:
            self.client = OpenAI(api_key=self.api_key, base_url=self.base_url)
            target_url = self.base_url or "https://api.openai.com/v1"
            self.logger.info(f"OpenAI 클라이언트 초기화 완료 (base_url={target_url}, model={self.model_name})")
        except Exception as e:
            self.logger.error(f"OpenAI 클라이언트 초기화 실패: {e}")
            self.client = None

    def generate_embedding(self, text: str) -> Optional[List[float]]:
        """
        Generate an embedding vector for the given text.
        
        May return None if the OpenAI client is not initialized or if embedding generation fails after retries.
        
        Returns:
            Optional[List[float]]: The embedding vector as a list of floats if successful, `None` otherwise.
        """
        if not self.client:
            self.logger.warning("OpenAI 클라이언트가 초기화되지 않았습니다.")
            return None

        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.client.embeddings.create(
                    model=self.model_name,
                    input=text,
                )
                embedding = response.data[0].embedding
                return embedding
            except RateLimitError as e:
                self.logger.warning(f"임베딩 생성 중 rate limit 발생 (시도 {attempt}/{self.max_retries}): {e}")
            except APIError as e:
                self.logger.error(f"임베딩 생성 중 API 오류 (시도 {attempt}/{self.max_retries}): {e}")
                # 5xx는 재시도, 그 외는 즉시 실패
                if getattr(e, "status_code", None) and getattr(e, "status_code") < 500:
                    break
            except Exception as e:
                self.logger.error(f"임베딩 생성 중 오류 (시도 {attempt}/{self.max_retries}): {e}")
                break

            if attempt < self.max_retries:
                sleep_time = self.retry_backoff * attempt
                time.sleep(sleep_time)

        return None

    def create_embedding_record(self,
                                preprocessed: ReviewPreprocessed,
                                source_content_type: str = 'preprocessed') -> Optional[ReviewEmbedding]:
        """
                                Create a ReviewEmbedding record from a preprocessed review's refined text.
                                
                                Attempts to generate an embedding for the review's refined_text and constructs a ReviewEmbedding (without persisting it) containing the app_review_id, source_content_type, model_name, and vector.
                                
                                Parameters:
                                    preprocessed (ReviewPreprocessed): The preprocessed review containing `refined_text` and `app_review_id`.
                                    source_content_type (str): The stage or origin of the text used for embedding (e.g., 'raw', 'preprocessed', 'features').
                                
                                Returns:
                                    ReviewEmbedding | None: A ReviewEmbedding instance populated with the generated vector if successful, `None` if `refined_text` is missing or embedding generation fails.
                                """
        if not preprocessed.refined_text:
            return None

        try:
            text = preprocessed.refined_text
            start_time = datetime.now()

            # 임베딩 생성
            vector = self.generate_embedding(text)
            if not vector:
                return None

            end_time = datetime.now()
            latency_ms = int((end_time - start_time).total_seconds() * 1000)

            # ReviewEmbedding 객체 생성 (matching DBinit.sql schema)
            embedding_record = ReviewEmbedding(
                app_review_id=preprocessed.app_review_id,  # FK to app_reviews.id
                source_content_type=source_content_type,
                model_name=self.model_name,
                vector=vector  # pgvector type
            )

            return embedding_record

        except Exception as e:
            self.logger.error(f"리뷰 {preprocessed.id} 임베딩 생성 중 오류: {e}")
            return None


    def process_batch(self, 
                     batch_size: int = 100, 
                     limit: Optional[int] = None,
                     stage: str = 'preprocessed'):
        """
                     Process preprocessed reviews in batches to generate embedding vectors and persist them as ReviewEmbedding records.
                     
                     Creates missing tables if needed, queries unprocessed ReviewPreprocessed rows (excluding reviews that already have embeddings), generates embeddings in batches, adds successful embedding records to the database session, commits after each batch, and logs progress and a summary of successes and failures. On error, the session is rolled back and the exception is re-raised. If the OpenAI client is not initialized, processing is aborted.
                     
                     Parameters:
                         batch_size (int): Number of reviews to process per database commit.
                         limit (Optional[int]): Optional cap on the total number of reviews to process.
                         stage (str): Source content stage to record on each ReviewEmbedding (e.g., 'preprocessed').
                     """
        self.logger.info("=" * 60)
        self.logger.info("임베딩 생성 파이프라인 시작")
        self.logger.info("=" * 60)

        if not self.client:
            self.logger.error("OpenAI 클라이언트가 초기화되지 않아 처리를 중단합니다.")
            return

        session = self.db_connector.get_session()

        try:
            # 테이블 생성
            self.db_connector.create_tables(Base)

            # 임베딩 생성할 전처리된 리뷰 조회
            query = session.query(ReviewPreprocessed)

            # 이미 임베딩 생성된 리뷰 제외
            try:
                processed_review_ids = session.query(ReviewEmbedding.app_review_id).distinct().all()
                processed_ids = {row[0] for row in processed_review_ids}

                if processed_ids:
                    query = query.filter(~ReviewPreprocessed.app_review_id.in_(processed_ids))
                    self.logger.info(f"이미 임베딩 생성된 리뷰 {len(processed_ids)}개 제외")
            except Exception as e:
                self.logger.warning(f"임베딩 조회 실패: {e}")

            if limit:
                query = query.limit(limit)

            total_reviews = query.count()
            self.logger.info(f"임베딩 생성할 리뷰 개수: {total_reviews}")

            if total_reviews == 0:
                self.logger.info("임베딩 생성할 리뷰가 없습니다.")
                return

            processed_count = 0
            failed_count = 0

            # 배치 처리
            for i in range(0, total_reviews, batch_size):
                batch = query.offset(i).limit(batch_size).all()

                for preprocessed in batch:
                    embedding_record = self.create_embedding_record(preprocessed, stage)
                    if embedding_record:
                        session.add(embedding_record)
                        processed_count += 1
                    else:
                        failed_count += 1

                # 배치 커밋
                session.commit()
                progress = min(i + batch_size, total_reviews)
                self.logger.info(f"진행률: {progress}/{total_reviews} ({progress/total_reviews*100:.1f}%)")

            self.logger.info("=" * 60)
            self.logger.info(f"✅ 임베딩 생성 완료")
            self.logger.info(f"   - 성공: {processed_count}개")
            self.logger.info(f"   - 실패: {failed_count}개")
            self.logger.info(f"   - 벡터 차원: {self.vector_dimension}")
            self.logger.info("=" * 60)

        except Exception as e:
            self.logger.error(f"임베딩 생성 중 오류 발생: {e}")
            session.rollback()
            raise
        finally:
            session.close()


def main():
    """
    Run the embedding generation pipeline from the command line.
    
    Parses optional command-line arguments: the first argument is interpreted as an integer limit of records to process (ignored if not an integer), and the second argument is the embedding model name (defaults to "text-embedding-3-small"). Instantiates an EmbeddingGenerator with the selected model and invokes process_batch(batch_size=100, limit=limit).
    """
    import sys

    limit = None
    model_name = 'text-embedding-3-small'

    if len(sys.argv) > 1:
        try:
            limit = int(sys.argv[1])
        except ValueError:
            pass

    if len(sys.argv) > 2:
        model_name = sys.argv[2]

    generator = EmbeddingGenerator(model_name=model_name)
    generator.process_batch(batch_size=100, limit=limit)


if __name__ == '__main__':
    main()