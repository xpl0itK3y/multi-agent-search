"""Backward-compat re-export for the API layer.

The models now live in src/domain/models.py (AUD-031). The api layer (and any external
caller) keeps importing them from here; non-api code imports from src.domain instead.
"""
from src.domain.models import *  # noqa: F401,F403
