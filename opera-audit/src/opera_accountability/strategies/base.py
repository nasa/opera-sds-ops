"""Abstract base class for accountability strategies."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Optional


class AccountabilityStrategy(ABC):
    """Abstract base class for accountability analysis strategies.
    
    Each strategy implements a specific accountability analysis approach:
    - forward_map: Maps input products to output products (e.g., HLS → DSWx-HLS)
    - date_count: Counts products by date and identifies gaps (e.g., TROPO)
    - delegated_validator: Delegates validation to external validator (e.g., DISP-S1)
    - db_based: Uses database for coverage checks (e.g., DISP-S1-STATIC)
    """
    
    @abstractmethod
    def analyze(
        self,
        start_date: datetime,
        end_date: datetime,
        venue: str = "PROD",
        **kwargs
    ) -> dict[str, Any]:
        """Run accountability analysis for the strategy.
        
        Args:
            start_date: Start of analysis period
            end_date: End of analysis period
            venue: CMR venue ('PROD' or 'UAT')
            **kwargs: Strategy-specific options
            
        Returns:
            Dict with analysis results including:
            - expected: Number of expected products
            - actual: Number of actual products
            - missing_count: Number of missing products
            - missing: List of missing product IDs
            - Additional strategy-specific fields
        """
        pass
    
    @abstractmethod
    def get_strategy_name(self) -> str:
        """Return the strategy name for CLI and reporting."""
        pass
    
    def validate_config(self, product: str) -> bool:
        """Validate that the product has required configuration for this strategy.
        
        Args:
            product: Product name from config.yaml
            
        Returns:
            True if configuration is valid, False otherwise
        """
        return True
