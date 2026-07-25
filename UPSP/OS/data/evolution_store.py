"""
进化集存储层 — Raw/Tacit + Raw/Connection 到 Materials/Evolution 的落盘闭环

data 层独占文件 I/O。自主轮成功写入进化集后，pending 原始行追加到
processed.jsonl 和本次批次备份，再原子清空 pending.jsonl。
"""
import json
import os
from datetime import datetime

from data.atomic_write import atomic_write_text
from paths import CONTAINER_ITERATION_DIR


class EvolutionStore:
    def __init__(self, iteration_dir=None):
        self.iteration_dir = iteration_dir or CONTAINER_ITERATION_DIR
        self.raw_dir = os.path.join(self.iteration_dir, "Raw")
        self.materials_dir = os.path.join(self.iteration_dir, "Materials")
        self.tacit_dir = os.path.join(self.raw_dir, "Tacit")
        self.connection_dir = os.path.join(self.raw_dir, "Connection")
        self.evolution_dir = os.path.join(self.materials_dir, "Evolution")

    def ensure_dirs(self):
        for path in (self.tacit_dir, self.connection_dir, self.evolution_dir):
            os.makedirs(path, exist_ok=True)

    def pending_paths(self):
        return {
            "tacit": os.path.join(self.tacit_dir, "pending.jsonl"),
            "connection": os.path.join(self.connection_dir, "pending.jsonl"),
        }

    def processed_paths(self):
        return {
            "tacit": os.path.join(self.tacit_dir, "processed.jsonl"),
            "connection": os.path.join(self.connection_dir, "processed.jsonl"),
        }

    def count_pending(self):
        self.ensure_dirs()
        return {
            name: len(self._read_lines(path))
            for name, path in self.pending_paths().items()
        }

    def should_trigger(self, thresholds):
        counts = self.count_pending()
        tacit_threshold = int(thresholds.get("tacit_pending_threshold", 512))
        connection_threshold = int(thresholds.get("connection_pending_threshold", 512))
        return (
            counts["tacit"] >= tacit_threshold and tacit_threshold > 0
        ) or (
            counts["connection"] >= connection_threshold and connection_threshold > 0
        )

    def load_pending(self):
        self.ensure_dirs()
        return {
            "tacit": self._read_records(self.pending_paths()["tacit"]),
            "connection": self._read_records(self.pending_paths()["connection"]),
        }

    def process_pending(self, evolution_text, round_num, stats=None):
        self.ensure_dirs()
        output_path = self.write_evolution(evolution_text, round_num, stats or {})
        self._move_pending_to_processed("tacit", round_num)
        self._move_pending_to_processed("connection", round_num)
        return output_path

    def write_evolution(self, evolution_text, round_num, stats):
        self.ensure_dirs()
        path = os.path.join(self.evolution_dir, f"evolution_R{round_num}.md")
        content = self._format_evolution(evolution_text, round_num, stats)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(content)
        os.replace(tmp, path)
        return path

    def _move_pending_to_processed(self, name, round_num):
        pending_path = self.pending_paths()[name]
        processed_path = self.processed_paths()[name]
        lines = self._read_lines(pending_path)
        if not lines:
            atomic_write_text(pending_path, "")
            return
        os.makedirs(os.path.dirname(processed_path), exist_ok=True)
        with open(processed_path, "a", encoding="utf-8") as f:
            for line in lines:
                f.write(line + "\n")
        batch_path = self._processed_batch_path(name, round_num)
        with open(batch_path, "a", encoding="utf-8") as f:
            for line in lines:
                f.write(line + "\n")
        atomic_write_text(pending_path, "")

    def _processed_batch_path(self, name, round_num):
        directory = self.tacit_dir if name == "tacit" else self.connection_dir
        stamp = datetime.now().strftime("%Y_%m_%d")
        return os.path.join(directory, f"processed_{stamp}_R{round_num}.jsonl")

    def _read_records(self, path):
        records = []
        for line in self._read_lines(path):
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                records.append({"_raw": line})
        return records

    def _read_lines(self, path):
        if not os.path.isfile(path):
            return []
        with open(path, "r", encoding="utf-8") as f:
            return [line.rstrip("\n") for line in f if line.strip()]

    def _format_evolution(self, evolution_text, round_num, stats):
        now = datetime.now().isoformat()
        stats_json = json.dumps(stats, ensure_ascii=False, indent=2)
        return (
            f"# 进化集 R{round_num}\n\n"
            f"- round: {round_num}\n"
            f"- created_at: {now}\n"
            f"- tacit_count: {stats.get('tacit_count', 0)}\n"
            f"- connection_count: {stats.get('connection_count', 0)}\n\n"
            "## 提炼\n\n"
            f"{evolution_text.strip()}\n\n"
            "## 统计\n\n"
            "```json\n"
            f"{stats_json}\n"
            "```\n"
        )
