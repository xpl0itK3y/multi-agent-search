"""Backward-compat re-export for the API layer.

The models now live in src/domain/models.py (AUD-031). The api layer (and any external
caller) keeps importing them from here; non-api code imports from src.domain instead.
The star import is bounded by src.domain.__all__ (the models and the service errors),
so the helpers models.py imports for itself are not re-exported.
"""
from src.domain import *  # noqa: F403
from src.domain import __all__  # noqa: F401  (the re-exported list)
