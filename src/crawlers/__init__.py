"""크롤러 패키지 초기화 및 팩토리."""
from enum import Enum
from typing import Optional, Union

from .appstore_crawler import AppStoreCrawler
from .playstore_crawler import PlayStoreCrawler
from .unified_crawler import UnifiedCrawler

__version__ = "1.0.0"


class Store(str, Enum):
    APPSTORE = "appstore"
    PLAYSTORE = "playstore"
    UNIFIED = "unified"


def get_crawler(store: Union[str, "Store"], config_path: Optional[str] = None):
    """
    Get a crawler instance for the specified store.
    
    Parameters:
        store (str | Store): Store identifier ('appstore', 'playstore', 'unified') or the corresponding Store enum; string values are case-insensitive.
        config_path (Optional[str]): Path to crawler configuration passed to the crawler constructor.
    
    Returns:
        AppStoreCrawler | PlayStoreCrawler | UnifiedCrawler: An instance of the crawler for the requested store.
    
    Raises:
        ValueError: If the provided store is not supported.
    """
    if isinstance(store, str):
        store = store.lower()
        try:
            store = Store(store)
        except ValueError as exc:
            raise ValueError(f"Unsupported store: {store}") from exc

    if store == Store.APPSTORE:
        return AppStoreCrawler(config_path)
    if store == Store.PLAYSTORE:
        return PlayStoreCrawler(config_path)
    if store == Store.UNIFIED:
        return UnifiedCrawler(config_path)
    raise ValueError(f"Unsupported store: {store}")


__all__ = ["Store", "get_crawler", "AppStoreCrawler", "PlayStoreCrawler", "UnifiedCrawler"]