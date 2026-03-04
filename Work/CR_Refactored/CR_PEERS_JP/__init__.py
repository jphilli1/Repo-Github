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

from .master_data_dictionary import (
    LOCAL_DERIVED_METRICS,
    MasterDataDictionary,
)

__all__ = [
    "MasterDataDictionary",
    "LOCAL_DERIVED_METRICS",
]
