import json


def test_memory_heat_tick_decay_uses_decay_calculator(tmp_path, monkeypatch):
    import data.memory_heat as memory_heat_mod
    from data.memory_heat import MemoryHeat
    from data.stm_heat_calculator import STMHeatCalculator

    heat_path = tmp_path / "heat.json"
    heat_path.write_text(json.dumps({
        "entries": {
            "MEM-00000001": {
                "H": 50,
                "zone": "未定",
                "AH_high": 0,
                "AH_low": 0,
                "degrade": False,
                "stored": False,
                "compression": False,
                "heat_locked": False,
            }
        }
    }, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(memory_heat_mod, "HEAT_JSON", str(heat_path))

    calls = {}

    def fake_tick_decay(self, entries, heat_locked_ids=None):
        calls["entries"] = entries
        return {"MEM-00000001": {"H": 41, "zone": "未定", "AH_low": 7}}

    monkeypatch.setattr(STMHeatCalculator, "tick_decay", fake_tick_decay)

    assert MemoryHeat().tick_decay() is True

    saved = json.loads(heat_path.read_text(encoding="utf-8"))
    assert calls["entries"]["MEM-00000001"]["H"] == 50
    assert saved["entries"]["MEM-00000001"]["H"] == 41
    assert saved["entries"]["MEM-00000001"]["AH_low"] == 7
