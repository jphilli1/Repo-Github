"""
CR_PEERS_JP — Refactored Credit Risk Dashboard Data Dictionary
==============================================================

Provides the :class:`MasterDataDictionary`, a single consolidated module that
replaces the previously fragmented MDRMValidator, MDRMClient,
EnhancedDataDictionaryClient, and hardcoded FDIC_FFIEC_Series_Key definitions.

Quick start::

    from CR_PEERS_JP import MasterDataDictionary

    mdd = MasterDataDictionary()
    print(mdd.lookup_metric("RCON2170"))
    report_df = mdd.export_dictionary_report()
"""

import sys as _sys, os as _os
_dp = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "src", "data_processing")
if _dp not in _sys.path:
    _sys.path.insert(0, _dp)

from master_data_dictionary import (
    LOCAL_DERIVED_METRICS,
    MasterDataDictionary,
)

__all__ = [
    "MasterDataDictionary",
    "LOCAL_DERIVED_METRICS",
]
