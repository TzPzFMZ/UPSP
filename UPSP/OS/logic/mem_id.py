"""
记忆编号生成器 — 8位十六进制 TTTTTNNN
DDS §9.2 记忆编号

格式: MEM-TTTTTNNN
  TTTTT（前5位）：当日零点起的秒数，0x00000 ~ 0x1517F（覆盖86400秒）
  NNN（后3位）：随机数，000 ~ FFF（4096种组合）

示例: MEM-0E6F3A7B
"""
import random
from datetime import datetime

from constants import TZ_SHANGHAI


def generate_mem_id():
    """生成新的 MEM-TTTTTNNN 编号"""
    now = datetime.now(TZ_SHANGHAI)
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    seconds = int((now - midnight).total_seconds())
    ttttt = format(seconds, '05X')
    nnn = format(random.randint(0, 0xFFF), '03X')
    return f"MEM-{ttttt}{nnn}"


def validate_mem_id(mem_id):
    """校验是否为合法 MEM-TTTTTNNN 格式"""
    import re
    return bool(re.match(r"^MEM-[0-9A-F]{8}$", mem_id))


# 元数据模板生成 —— 保留到 logic/ 因为它是纯计算（不需要读文件）
def make_meta_template(mem_id, title="", weight=2, subject=None, model=""):
    """创建 20 字段元数据 dict"""
    now = datetime.now(TZ_SHANGHAI).isoformat()
    return {
        "id": mem_id,
        "type": "F" if weight >= 5 else "S" if weight >= 3 else "A",
        "weight": weight,
        "title": title[:16] if title else mem_id,
        "dream": False,
        "created_at": now,
        "last_recalled_at": now,
        "created_round": None,
        "last_recalled_round": None,
        "source": "",
        "model": model,
        "subject": subject,
        "access": "public",
        "recalled": False,
        "current_overview": "",
        "tags": [],
        "linked_containers": [],
        "decay_period_days": 30,
        "decay_countdown_days": 30,
        "media": [],
    }


def make_heat_entry(weight=2):
    """创建 heat.json 条目（含遗忘分流三元数据）"""
    now = datetime.now(TZ_SHANGHAI).isoformat()
    return {
        "H": 50,
        "zone": "未定",
        "AH_high": 0,
        "AH_low": 0,
        "last_heat_at": now,
        "last_high_at": None,
        "degrade": False,
        "compression": weight >= 3,
        "stored": False,
        "heat_locked": False,
    }
