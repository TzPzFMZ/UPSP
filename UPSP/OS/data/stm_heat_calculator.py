"""
STM heat calculation only.

This module is the single source for STM heat decay, recall boost,
upgrade candidate checks, and STM forgetting split decisions. It does
not read or write files; persistence remains in data/memory_heat.py.
"""
class STMHeatCalculator:
    """Pure STM heat calculator."""

    def __init__(self, config=None):
        if config is None:
            from schemas.config import load_os_template_config
            config = load_os_template_config("memory.json")["heat"]
        self.config = config

    def tick_decay(self, heat, heat_locked_ids=None):
        """
        Return updated fields for one STM heat decay tick.

        heat: {mem_id: {"H": int, "zone": str, "heat_locked": bool, ...}, ...}
        heat_locked_ids: optional set of STM heat-locked mem_ids.
        """
        heat_locked = set(heat_locked_ids or [])
        updates = {}

        for mem_id, info in heat.items():
            if mem_id in heat_locked or info.get("heat_locked"):
                ah_high = info.get("AH_high", 0) + 1
                updates[mem_id] = {
                    "H": self.config["locked_value"],
                    "zone": "显著",
                    "AH_high": ah_high,
                    "AH_low": 0,
                }
                continue

            h = info.get("H", self.config["initial_by_weight"]["2"])
            zone = info.get("zone", "未定")

            decay = {
                "显著": self.config["decay_rates"]["significant"],
                "未定": self.config["decay_rates"]["uncertain"],
                "衰减": self.config["decay_rates"]["decay"],
            }[zone]
            new_h = max(0, h + decay)
            new_zone = self._reclassify_zone(new_h)

            if new_h != h or new_zone != zone:
                updates.setdefault(mem_id, {})
                updates[mem_id]["H"] = new_h
                updates[mem_id]["zone"] = new_zone

            if new_zone == "显著":
                ah_high = info.get("AH_high", 0) + 1
                updates.setdefault(mem_id, {})["AH_high"] = ah_high

            if new_zone == "衰减":
                ah_low = info.get("AH_low", 0) + 1
                updates.setdefault(mem_id, {})["AH_low"] = ah_low
                if ah_low >= 3 and not info.get("degrade"):
                    updates.setdefault(mem_id, {})["degrade"] = True

            if mem_id in updates:
                updates[mem_id].setdefault("H", new_h)
                updates[mem_id].setdefault("zone", new_zone)

        return updates

    def recall_boost(self, h, ah_high, boost=None):
        """Return heat after recall; AH_high is only settled by tick_decay."""
        boost = self.config["recall_boost"] if boost is None else boost
        new_h = min(100, h + boost)
        new_zone = self._reclassify_zone(new_h)
        return (new_h, ah_high, new_zone)

    def check_upgrade(self, heat, meta):
        """Return STM entries whose canonical LTM should be admitted."""
        candidates = []
        for mem_id, info in heat.items():
            if info.get("AH_high", 0) < self.config["upgrade_high_rounds"]:
                continue
            if not str(meta.get(mem_id, {}).get("stored_at") or "").strip():
                candidates.append(mem_id)
        return candidates

    def process_forgetting(self, heat, meta=None):
        """
        Split degraded STM entries into deletion, direct abstract, and LLM compression.

        Returns: (to_delete_stm, to_move_to_abstract, need_llm_compress)
        """
        to_delete = []
        to_abstract = []
        need_compress = []

        for mem_id, info in heat.items():
            if not info.get("degrade"):
                continue

            stored = bool(str((meta or {}).get(mem_id, {}).get("stored_at") or "").strip())
            compression = info.get("compression", False)

            if stored:
                to_delete.append(mem_id)
            elif not compression:
                to_abstract.append(mem_id)
            else:
                need_compress.append(mem_id)

        return (to_delete, to_abstract, need_compress)

    def weight_to_type(self, weight):
        """Map memory weight to LTM type marker."""
        if weight >= 5:
            return "F"
        if weight >= 3:
            return "S"
        return "A"

    def _reclassify_zone(self, h):
        if h >= self.config["zone_thresholds"]["significant"]:
            return "显著"
        if h >= self.config["zone_thresholds"]["uncertain"]:
            return "未定"
        return "衰减"
