# -*- coding: utf-8 -*-
"""
Text preprocessing pipeline
Based on colleague's preprocessing logic from REFERENCE/review_analysis_pipeline.py
"""
from datetime import datetime
from typing import Optional
import json
import os
import re
import emoji

from src.utils.logger import get_logger
from src.utils.db_connector import DatabaseConnector
from src.models.base import Base
from src.models.review import Review
from src.models.review_preprocessed import ReviewPreprocessed


class TextPreprocessor:
    """
    Text preprocessing class

    Implements preprocessing logic from REFERENCE/review_analysis_pipeline.py
    1. Synonym replacement (domain_synonyms.json)
    2. Emoji processing (emoji.demojize)
    3. URL/HTML tag removal
    4. Repeated character normalization
    5. Profanity masking (profanity_map.json)
    """

    def __init__(self, config_path: str = 'config/crawler_config.yml'):
        self.logger = get_logger(__name__)
        self.db_connector = DatabaseConnector(config_path)
        self.preprocessing_version = '1.0'

        # Load dictionaries (DB first, file fallback)
        self.synonym_map = self._load_synonyms_from_db() or self._load_synonyms()
        self.profanity_map = self._load_profanity_from_db() or self._load_profanity()
        self.stopwords = self._load_stopwords()

    def _load_synonyms_from_db(self) -> dict:
        """Load domain synonyms from database (preferred method)"""
        try:
            from src.models.dictionary import Synonym

            session = self.db_connector.get_session()
            synonyms = session.query(Synonym).filter(Synonym.is_active == True).all()
            session.close()

            if not synonyms:
                self.logger.warning("No synonyms found in database")
                return {}

            # Convert to dictionary format (variant_form → canonical_form)
            synonym_dict = {s.variant_form: s.canonical_form for s in synonyms}
            self.logger.info(f"✅ Loaded {len(synonym_dict)} synonym mappings from DATABASE")
            return synonym_dict

        except Exception as e:
            self.logger.warning(f"Failed to load synonyms from DB: {e}")
            return {}

    def _load_synonyms(self) -> dict:
        """Load domain synonyms dictionary from file (fallback method)"""
        try:
            path = 'config/dictionaries/domain_synonyms.json'
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            self.logger.info(f"⚠️  Loaded {len(data)} synonym mappings from FILE (fallback)")
            return data
        except Exception as e:
            self.logger.warning(f"Failed to load synonyms from file: {e}")
            return {}

    def _load_profanity_from_db(self) -> dict:
        """Load profanity mapping from database (DB + file hybrid)

        Note: DB only stores word and severity_level, not replacement tags.
        We load active profanities from DB and map to replacements from file.
        """
        try:
            from src.models.dictionary import Profanity

            # Load profanity list from DB
            session = self.db_connector.get_session()
            profanities = session.query(Profanity).filter(Profanity.is_active == True).all()
            session.close()

            if not profanities:
                self.logger.warning("No profanities found in database")
                return {}

            # Load replacement mappings from file
            try:
                path = 'config/dictionaries/profanity_map.json'
                with open(path, 'r', encoding='utf-8') as f:
                    file_replacements = json.load(f)
            except Exception as e:
                self.logger.warning(f"Failed to load profanity replacements from file: {e}")
                file_replacements = {}

            # Combine: word → replacement (from file, filtered by DB active list)
            active_words = {p.word for p in profanities}
            profanity_dict = {word: replacement
                             for word, replacement in file_replacements.items()
                             if word in active_words}

            self.logger.info(f"✅ Loaded {len(profanity_dict)} profanity mappings from DATABASE (with file replacements)")
            return profanity_dict

        except Exception as e:
            self.logger.warning(f"Failed to load profanities from DB: {e}")
            return {}

    def _load_profanity(self) -> dict:
        """Load profanity mapping dictionary from file (fallback method)"""
        try:
            path = 'config/dictionaries/profanity_map.json'
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            self.logger.info(f"⚠️  Loaded {len(data)} profanity mappings from FILE (fallback)")
            return data
        except Exception as e:
            self.logger.warning(f"Failed to load profanity map from file: {e}")
            return {}

    def _load_stopwords(self) -> list:
        """Load stopwords list"""
        try:
            path = 'config/dictionaries/stopwords.txt'
            with open(path, 'r', encoding='utf-8') as f:
                words = [line.strip() for line in f if line.strip()]
            self.logger.info(f"Loaded {len(words)} stopwords")
            return words
        except Exception as e:
            self.logger.warning(f"Failed to load stopwords: {e}")
            return []

    def preprocess_text(self, text: str) -> str:
        """
        Preprocess text (based on colleague's preprocess_for_model function)

        Steps (from REFERENCE/review_analysis_pipeline.py lines 64-75):
        1. Synonym replacement
        2. Emoji demojize
        3. URL/HTML removal
        4. Repeated character normalization
        5. Profanity masking
        """
        if not text:
            return ""

        processed_text = str(text)

        # 1. Synonym replacement
        for original, standard in self.synonym_map.items():
            processed_text = processed_text.replace(original, standard)

        # 2. Emoji processing
        processed_text = emoji.demojize(processed_text, language='ko')

        # 3. URL and HTML tag removal
        processed_text = re.sub(r'https?:\/\/\S+|www\.\S+|<.*?>', '', processed_text)

        # 4. Repeated character normalization
        processed_text = re.sub(r'([ㄱ-ㅎㅏ-ㅣㅋㅎㅠㅜ])\1{2,}', r'\1\1', processed_text)

        # 5. Profanity masking
        for word, mask in self.profanity_map.items():
            processed_text = processed_text.replace(word, mask)

        return processed_text.strip()

    def create_preprocessed_record(self, review: Review) -> Optional[ReviewPreprocessed]:
        """Create preprocessed review record"""
        if not review.review_text:
            return None

        try:
            original_content = review.review_text
            cleaned_content = self.preprocess_text(original_content)

            # Further normalization (whitespace, etc)
            refined_text = ' '.join(cleaned_content.split())

            # Create ReviewPreprocessed object (matching DBinit.sql schema)
            preprocessed = ReviewPreprocessed(
                app_review_id=review.id,  # FK to app_reviews.id
                refined_text=refined_text
            )

            return preprocessed

        except Exception as e:
            self.logger.error(f"Error preprocessing review {review.id}: {e}")
            return None

    def process_batch(self, batch_size: int = 100, limit: Optional[int] = None):
        """Batch processing - preprocess raw reviews"""
        self.logger.info("=" * 60)
        self.logger.info("Text Preprocessing Pipeline Started")
        self.logger.info("=" * 60)

        session = self.db_connector.get_session()

        try:
            # Create tables
            self.db_connector.create_tables(Base)

            # Query reviews to preprocess
            query = session.query(Review)

            # Exclude already preprocessed reviews
            try:
                processed_review_ids = session.query(ReviewPreprocessed.app_review_id).distinct().all()
                processed_ids = {row[0] for row in processed_review_ids}

                if processed_ids:
                    query = query.filter(~Review.id.in_(processed_ids))
                    self.logger.info(f"Excluding {len(processed_ids)} already preprocessed reviews")
            except Exception as e:
                self.logger.warning(f"Failed to query preprocessed reviews: {e}")

            if limit:
                query = query.limit(limit)

            total_reviews = query.count()
            self.logger.info(f"Reviews to preprocess: {total_reviews}")

            if total_reviews == 0:
                self.logger.info("No reviews to preprocess")
                return

            processed_count = 0
            failed_count = 0

            # Import App model for join query
            from src.models.app import App

            # Batch processing
            for i in range(0, total_reviews, batch_size):
                batch = query.offset(i).limit(batch_size).all()

                for review in batch:
                    preprocessed = self.create_preprocessed_record(review)
                    if preprocessed:
                        session.add(preprocessed)
                        processed_count += 1
                    else:
                        failed_count += 1

                # Commit batch
                session.commit()
                progress = min(i + batch_size, total_reviews)
                self.logger.info(f"Progress: {progress}/{total_reviews} ({progress/total_reviews*100:.1f}%)")

            self.logger.info("=" * 60)
            self.logger.info("Text Preprocessing Completed")
            self.logger.info(f"   - Success: {processed_count} reviews")
            self.logger.info(f"   - Failed: {failed_count} reviews")
            self.logger.info("=" * 60)

        except Exception as e:
            self.logger.error(f"Error during preprocessing: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    def get_reviews_to_preprocess(self, batch_size: int = 100):
        """Get reviews ready for preprocessing (Phase 3: NAS-first Architecture).

        Only returns reviews that are:
        - Status: RAW (successfully ingested to Bronze)
        - Parquet written (parquet_written_at IS NOT NULL)

        This ensures Silver layer only processes reviews with confirmed
        Parquet writes, preventing Ghost Records issues.

        Args:
            batch_size: Number of reviews to fetch

        Returns:
            List[ReviewMasterIndex]: Reviews ready for preprocessing
        """
        from src.models.review_master_index import ReviewMasterIndex
        from src.models.enums import ProcessingStatusType

        session = self.db_connector.get_session()

        try:
            reviews = session.query(ReviewMasterIndex).filter(
                ReviewMasterIndex.processing_status == ProcessingStatusType.RAW,
                ReviewMasterIndex.parquet_written_at.isnot(None),  # Parquet confirmed
                ReviewMasterIndex.is_active == True
            ).order_by(
                ReviewMasterIndex.ingested_at.asc()
            ).limit(batch_size).all()

            self.logger.info(
                f"Found {len(reviews)} reviews ready for preprocessing "
                f"(status=RAW, parquet_written=True)"
            )

            return reviews

        finally:
            session.close()


def main():
    """Main execution function"""
    import sys

    limit = None
    if len(sys.argv) > 1:
        try:
            limit = int(sys.argv[1])
        except ValueError:
            pass

    preprocessor = TextPreprocessor()
    preprocessor.process_batch(batch_size=100, limit=limit)


if __name__ == '__main__':
    main()
