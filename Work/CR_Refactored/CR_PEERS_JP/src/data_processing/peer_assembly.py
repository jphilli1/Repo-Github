"""
Peer Group Definitions & Assembly
==================================

Canonical peer group definitions for MSPBNA credit risk reporting.
Contains:
  - ``PeerGroupType`` enum
  - ``PEER_GROUPS`` dict (4 active groups: Core PB + All Peers x Standard/Normalized)
  - ``validate_peer_group_uniqueness()`` — enforcement that no two groups
    within the same normalization mode share identical cert lists
  - ``get_all_peer_certs()`` — union of all distinct CERTs across groups

Composite CERTs are auto-assigned: ``base_dummy_cert + display_order``.
Standard base = 90000, so Core PB = 90001, All Peers = 90003, etc.

**Subject bank excluded from wealth composites:** Core PB composites average
external peers only (GS + UBS). MSPBNA is NOT a member of its own peer group.
"""

from __future__ import annotations

import os
from enum import Enum
from typing import Dict, List

# ---------------------------------------------------------------------------
#  CERTs — loaded from env with safe defaults (import-safe)
# ---------------------------------------------------------------------------
MSPBNA_CERT = int(os.getenv("MSPBNA_CERT", "34221"))
MSBNA_CERT = int(os.getenv("MSBNA_CERT", "32992"))
MS_COMBINED_CERT = int(os.getenv("MS_COMBINED_CERT", "88888"))


# ---------------------------------------------------------------------------
#  Peer Group Enum
# ---------------------------------------------------------------------------

class PeerGroupType(str, Enum):
    CORE_PRIVATE_BANK = "Core_Private_Bank"
    MS_FAMILY_PLUS = "MS_Family_Plus"
    ALL_PEERS = "All_Peers"
    # Normalized Peer Groups (Ex-Commercial/Ex-Consumer view)
    CORE_PRIVATE_BANK_NORM = "Core_Private_Bank_Norm"
    MS_FAMILY_PLUS_NORM = "MS_Family_Plus_Norm"
    ALL_PEERS_NORM = "All_Peers_Norm"


# ---------------------------------------------------------------------------
#  Peer Group Definitions
# ---------------------------------------------------------------------------

PEER_GROUPS: Dict[PeerGroupType, dict] = {
    PeerGroupType.CORE_PRIVATE_BANK: {
        "name": "Core Private Bank Peers",
        "short_name": "Core PB",
        "description": "True private banking comparables - SBL, wealth management, UHNW focus",
        "certs": [33124, 57565],  # GS + UBS (external peers only)
        "use_case": "Best for SBL/wealth product comparisons, NCO benchmarking",
        "display_order": 1,
        "use_normalized": False,
    },
    # MS_FAMILY_PLUS removed: identical cert set to CORE_PRIVATE_BANK
    PeerGroupType.ALL_PEERS: {
        "name": "Full Peer Universe",
        "short_name": "All Peers",
        "description": "Complete peer set including MSBNA and G-SIBs for size/scale context",
        "certs": [MSBNA_CERT, 33124, 57565, 628, 3511, 7213, 3510],
        "use_case": "Regulatory comparison, market share analysis, full industry context",
        "display_order": 3,
        "use_normalized": False,
    },
    # ======================================================================
    # NORMALIZED PEER GROUPS (Ex-Commercial/Ex-Consumer)
    # ======================================================================
    PeerGroupType.CORE_PRIVATE_BANK_NORM: {
        "name": "Core Private Bank (Normalized)",
        "short_name": "Core PB Norm",
        "description": "Core PB peers with normalized metrics",
        "certs": [33124, 57565],  # GS + UBS (matches CORE_PRIVATE_BANK)
        "use_case": "True private bank comparison excluding mass market and commercial segments",
        "display_order": 4,
        "use_normalized": True,
    },
    # MS_FAMILY_PLUS_NORM removed: identical cert set to CORE_PRIVATE_BANK_NORM
    PeerGroupType.ALL_PEERS_NORM: {
        "name": "Full Peer Universe (Normalized)",
        "short_name": "All Peers Norm",
        "description": "All peers with normalized metrics for private bank comparable view",
        "certs": [MSBNA_CERT, 33124, 57565, 628, 3511, 7213, 3510],
        "use_case": "Broad comparison on normalized (ex-commercial/ex-consumer) basis",
        "display_order": 6,
        "use_normalized": True,
    },
}


# ---------------------------------------------------------------------------
#  Validation & helpers
# ---------------------------------------------------------------------------

def validate_peer_group_uniqueness(peer_groups: Dict = None) -> None:
    """Raise ValueError if two groups with the same use_normalized flag
    have identical sorted cert membership."""
    if peer_groups is None:
        peer_groups = PEER_GROUPS
    seen: dict = {}
    for gk, gv in peer_groups.items():
        norm_flag = gv.get("use_normalized", False)
        cert_key = (norm_flag, tuple(sorted(gv["certs"])))
        if cert_key in seen:
            raise ValueError(
                f"Peer group '{gk}' has identical cert membership as '{seen[cert_key]}' "
                f"(use_normalized={norm_flag}, certs={sorted(gv['certs'])}). "
                f"Remove the duplicate or provide distinct membership."
            )
        seen[cert_key] = gk


def get_all_peer_certs(peer_groups: Dict = None) -> List[int]:
    """Return union of all distinct CERTs mentioned in any peer group."""
    if peer_groups is None:
        peer_groups = PEER_GROUPS
    all_certs: set = set()
    for group in peer_groups.values():
        all_certs.update(group["certs"])
    return list(all_certs)
