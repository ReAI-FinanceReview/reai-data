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
        """
        Initialize the TextPreprocessor and load preprocessing resources.
        
        Parameters:
            config_path (str): Path to the crawler configuration YAML used to initialize the database connector and locate dictionary files.
        
        Description:
            Creates a logger and a DatabaseConnector, sets the preprocessing version, and loads dictionaries:
            - synonym_map: domain synonyms (database-first, file fallback)
            - profanity_map: profanity replacements (database-first, file fallback)
            - stopwords: list of stopwords loaded from file
        
        Attributes set:
            logger, db_connector, preprocessing_version, synonym_map, profanity_map, stopwords
        """
        self.logger = get_logger(__name__)
        self.db_connector = DatabaseConnector(config_path)
        self.preprocessing_version = '1.0'

        # Load dictionaries (DB first, file fallback)
        self.synonym_map = self._load_synonyms_from_db() or self._load_synonyms()
        self.profanity_map = self._load_profanity_from_db() or self._load_profanity()
        self.stopwords = self._load_stopwords()

    def _load_synonyms_from_db(self) -> dict:
        """
        Load domain synonym mappings from the database.
        
        Returns:
            dict: Mapping of variant form to canonical form (e.g., `{variant_form: canonical_form}`). Returns an empty dict if no active synonyms are found or if loading fails.
        """
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
        """
        Load domain synonym mappings from the file config/dictionaries/domain_synonyms.json.
        
        Returns:
            dict: Mapping of variant forms to canonical forms; an empty dict if the file cannot be read or parsing fails.
        """
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
        """
        Load active profanity words from the database and map each to its replacement string from the profanity mapping file.
        
        Returns:
            dict: Mapping of profane word to replacement string. Returns an empty dict if no active profanities are found or if loading fails.
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
        """
        Load profanity-to-replacement mappings from the file fallback.
        
        @returns dict: Mapping from profane word to replacement string. Returns an empty dict if the file cannot be read or parsed.
        """
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
        """
        Load stopwords from the file config/dictionaries/stopwords.txt.
        
        Reads the file and returns a list of non-empty lines with surrounding whitespace removed. If the file cannot be read or an error occurs, an empty list is returned.
        Returns:
            list: Stopwords as stripped strings; empty list on failure.
        """
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
        Apply a sequence of text preprocessing transformations: synonym replacement, emoji demojization (Korean), URL and HTML tag removal, repeated-character normalization, and profanity masking.
        
        The function performs the pipeline in that order and strips surrounding whitespace from the result.
        
        Returns:
        	The preprocessed text with the transformations applied.
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
        """
        Create a ReviewPreprocessed instance from a Review by applying the text preprocessing pipeline.
        
        Parameters:
            review (Review): The source review object whose `review_text` will be preprocessed.
        
        Returns:
            ReviewPreprocessed or None: A `ReviewPreprocessed` with `refined_text` set to the normalized, preprocessed text, or `None` if the review has no text or preprocessing fails.
        """
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
        """
        Process reviews in batches and persist their preprocessed records.
        
        This method finds reviews that have not yet been preprocessed, applies the preprocessing pipeline to each, creates and stores ReviewPreprocessed records, and commits results to the database in batches. Progress and a final summary of successes and failures are logged. On error the database transaction is rolled back and the exception is propagated.
        
        Parameters:
            batch_size (int): Number of reviews to process and commit per batch.
            limit (Optional[int]): Optional maximum number of reviews to process; if None, all pending reviews are processed.
        """
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


def main():
    """
    Entry point for command-line execution.
    
    Parses an optional first positional argument as an integer limit (invalid values are ignored), instantiates TextPreprocessor, and runs batch processing with batch_size=100 and the parsed limit.
    """
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