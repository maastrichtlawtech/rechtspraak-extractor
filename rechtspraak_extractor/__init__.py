from rechtspraak_extractor import rechtspraak
from rechtspraak_extractor import rechtspraak_metadata
from rechtspraak_extractor import rechtspraak_functions
from rechtspraak_extractor.rechtspraak import get_rechtspraak
from rechtspraak_extractor.rechtspraak_metadata import get_rechtspraak_metadata
import logging
logging.basicConfig(level=logging.INFO)

__all__ = ['get_rechtspraak', 'get_rechtspraak_metadata', 'rechtspraak', 'rechtspraak_metadata', 'rechtspraak_functions']
