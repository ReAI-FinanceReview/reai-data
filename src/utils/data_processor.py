"""
데이터 처리 유틸리티
"""
import pandas as pd
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional
from ..models.review import Review


class DataProcessor:
    """데이터 처리 클래스"""
    
    @staticmethod
    def flatten_entry(entry: Dict[str, Any], parent_key: str = '', sep: str = '.') -> Dict[str, Any]:
        """중첩된 딕셔너리를 평면화"""
        items = []
        for k, v in entry.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(DataProcessor.flatten_entry(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)
    
    @staticmethod
    def normalize_appstore_review(entry: Dict[str, Any], app_id: str, app_name: str) -> Dict[str, Any]:
        """App Store 리뷰 데이터 정규화"""
        flat = DataProcessor.flatten_entry(entry)
        flat['review_id'] = str(uuid.uuid4())
        flat['app_id'] = app_id
        flat['app_name'] = app_name
        flat['platform'] = 'APPSTORE'
        return flat
    
    @staticmethod
    def normalize_playstore_review(review_data: Dict[str, Any], app_id: str) -> Dict[str, Any]:
        """Play Store 리뷰 데이터를 App Store 형식과 일치하도록 정규화"""
        # 기본 메타데이터
        normalized_review = {
            'review_id': str(uuid.uuid4()),
            'app_id': app_id,
            'platform': 'PLAYSTORE',
            'app_name': f'app_{app_id}'
        }
        
        # Play Store 필드를 App Store 형식에 맞게 매핑
        field_mapping = {
            'userName': 'author.name.label',
            'content': 'content.label',
            'score': 'im:rating.label',
            'at': 'updated.label',
            'reviewCreatedVersion': 'im:version.label',
            'thumbsUpCount': 'im:voteSum.label',
            'reviewId': 'id.label',
            'userImage': 'author.uri.label'
        }
        
        for play_field, app_field in field_mapping.items():
            if play_field in review_data:
                value = review_data[play_field]
                if play_field in ['score', 'thumbsUpCount'] and value is not None:
                    value = str(value)
                elif play_field == 'at' and value is not None:
                    # 날짜 형식 처리
                    if hasattr(value, 'isoformat'):
                        value = value.isoformat()
                normalized_review[app_field] = value
        
        # Play Store 고유 필드들도 보존
        playstore_fields = ['replyContent', 'repliedAt', 'appVersion']
        for field in playstore_fields:
            if field in review_data:
                normalized_review[field] = review_data[field]
        
        # App Store와 일치하는 기본값들
        default_fields = {
            'content.attributes.type': 'text',
            'link.attributes.rel': 'related',
            'link.attributes.href': f'https://play.google.com/store/apps/details?id={app_id}',
            'im:contentType.attributes.term': 'Application',
            'im:contentType.attributes.label': '앱',
            'author.label': '',
            'im:voteCount.label': normalized_review.get('im:voteSum.label', '0')
        }
        
        normalized_review.update(default_fields)
        
        # title 생성 (content 기반)
        if 'content.label' in normalized_review and normalized_review['content.label']:
            content = normalized_review['content.label']
            normalized_review['title.label'] = content[:100] + '...' if len(content) > 100 else content
        
        return normalized_review
    
    @staticmethod
    def create_unified_dataframe(reviews: List[Dict[str, Any]]) -> pd.DataFrame:
        """통합 데이터프레임 생성"""
        if not reviews:
            return pd.DataFrame()
        
        df = pd.DataFrame(reviews)
        
        # 메타데이터 컬럼을 앞쪽에 배치
        meta_cols = ['review_id', 'app_id', 'app_name', 'platform']
        other_cols = [col for col in df.columns if col not in meta_cols]
        ordered_cols = meta_cols + other_cols
        
        # 존재하는 컬럼만 선택
        existing_cols = [col for col in ordered_cols if col in df.columns]
        
        return df[existing_cols]
    
    @staticmethod
    def clean_text(text: str) -> str:
        """텍스트 정리"""
        if not isinstance(text, str):
            return str(text) if text is not None else ""
        
        # 기본적인 텍스트 정리
        text = text.strip()
        text = text.replace('\n', ' ').replace('\r', ' ')
        text = ' '.join(text.split())  # 연속된 공백 제거
        
        return text
    
    @staticmethod
    def extract_app_info(apps_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """앱 정보 추출"""
        app_info = []
        for app_data in apps_data:
            if 'app_id' in app_data and 'app_name' in app_data:
                info = {
                    'app_id': app_data['app_id'],
                    'app_name': app_data['app_name'],
                    'platform': app_data.get('platform'),
                    'last_crawled': datetime.now().isoformat()
                }
                app_info.append(info)
        
        return pd.DataFrame(app_info).drop_duplicates(['app_id', 'platform'])
