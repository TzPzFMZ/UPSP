"""UPSP Base — 程序路径与当前活动位格路径的唯一投影。

``PROGRAM_OS_ROOT`` 永远是安装目录中的后端代码；``OS_ROOT`` 永远是
Documents 数据根中当前 PID 的 OS。其他模块不得重新拼接活动实例路径。
"""
import os
import sys

# ============================================================
# 程序根、Windows 用户数据根与活动实例
# ============================================================

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PROGRAM_OS_ROOT = _THIS_DIR
PROGRAM_UPSP_ROOT = os.path.dirname(PROGRAM_OS_ROOT)
UPSP_ROOT = PROGRAM_UPSP_ROOT  # 兼容：UPSP_ROOT 始终表示程序产品根
if PROGRAM_UPSP_ROOT not in sys.path:
    sys.path.insert(0, PROGRAM_UPSP_ROOT)

from initialization.windows_data import load_active_instance  # noqa: E402

_ACTIVE_LAYOUT = load_active_instance(PROGRAM_UPSP_ROOT)
ACTIVE_PID = _ACTIVE_LAYOUT.pid
UPSP_DATA_ROOT = str(_ACTIVE_LAYOUT.data_root)
UPSP_LOCAL_STATE_ROOT = str(_ACTIVE_LAYOUT.local_state_root)
ACTIVE_INSTANCE_MANIFEST = str(_ACTIVE_LAYOUT.manifest_path)
ACTIVE_INSTANCE_ROOT = str(_ACTIVE_LAYOUT.instance_root)
OS_ROOT = str(_ACTIVE_LAYOUT.os_root)

INITIALIZATION_DIR = os.path.join(PROGRAM_UPSP_ROOT, "initialization")
PERSONA_TEMPLATE_DIR = os.path.join(INITIALIZATION_DIR, "persona_template")
PERSONA_PRESETS_DIR = os.path.join(INITIALIZATION_DIR, "persona_presets")
OS_TEMPLATE_DIR = os.path.join(INITIALIZATION_DIR, "os_template")

# ============================================================
# 一级目录
# ============================================================

def resolve_persona_dir():
    """Return the one active persona root selected by the manifest."""
    return str(_ACTIVE_LAYOUT.persona_dir)


PERSONA_DIR = resolve_persona_dir()                       # 位格核心（内环境）
GLOBAL_CONFIG_DIR = str(_ACTIVE_LAYOUT.global_config_dir) # 本机跨位格配置
CONFIG_DIR    = str(_ACTIVE_LAYOUT.config_dir)            # 当前位格配置
ORGAN_TOPOLOGY = os.path.join(CONFIG_DIR, "organ_topology.json")
ENGINES_DIR   = os.path.join(PROGRAM_OS_ROOT, "engines")  # 运行编排层
SCRIPTS_DIR   = os.path.join(PROGRAM_OS_ROOT, "scripts")  # 维护/审计脚本区
LOGIC_DIR     = os.path.join(PROGRAM_OS_ROOT, "logic")    # 业务逻辑层
DATA_DIR      = os.path.join(PROGRAM_OS_ROOT, "data")     # 数据访问层
ASSEMBLY_DIR  = os.path.join(PROGRAM_OS_ROOT, "assembly") # 上下文装配层
SCHEMAS_DIR   = os.path.join(PROGRAM_OS_ROOT, "schemas")  # 数据格式定义层
TESTS_DIR     = os.path.join(PROGRAM_OS_ROOT, "tests")    # 测试
ADAPTERS_DIR  = os.path.join(PROGRAM_OS_ROOT, "adapters") # 外部适配器
TRASH_DIR     = str(_ACTIVE_LAYOUT.trash_dir)              # 当前位格垃圾桶
FILES_DIR     = str(_ACTIVE_LAYOUT.files_dir)              # 当前位格资料暂存区
FILES_RAW_DIR = os.path.join(FILES_DIR, "raw")        # 原始外部文件
FILES_MEDIA_RAW_DIR = os.path.join(FILES_DIR, "media_raw")  # 原始多媒体文件
FILES_CLIPS_DIR = os.path.join(FILES_DIR, "clips")    # LLM剪贴材料
FILES_ARCHIVE_DIR = os.path.join(FILES_DIR, "archive")  # 留档原文件

# tracked HTML 属于程序；可再生 round-index/data 属于本机缓存。
AUDIT_HTML_DIR = os.path.join(PROGRAM_OS_ROOT, "audit")
AUDIT_DIR = str(_ACTIVE_LAYOUT.audit_cache_dir)

# ============================================================
# persona/ 内环境 — 七文件
# ============================================================

CORE_MD    = os.path.join(PERSONA_DIR, "core.md")
STATE_JSON = os.path.join(PERSONA_DIR, "state.json")

# ============================================================
# persona/rules/
# ============================================================

RULES_DIR      = os.path.join(PERSONA_DIR, "rules")
RULES_REGISTRY = os.path.join(RULES_DIR, "rules_registry.json")

# ============================================================
# persona/docs/
# ============================================================

DOCS_DIR      = os.path.join(PERSONA_DIR, "docs")
DOCS_REGISTRY = os.path.join(DOCS_DIR, "docs_registry.json")

# docs/protocol/base/ — 协议层
DOCS_PROTOCOL_BASE_DIR = os.path.join(DOCS_DIR, "protocol", "base")
DOCS_CORE_AXIS         = os.path.join(DOCS_PROTOCOL_BASE_DIR, "core.md")
DOCS_DYNAMIC_AXIS      = os.path.join(DOCS_PROTOCOL_BASE_DIR, "dynamic.md")
DOCS_TERMINOLOGY       = os.path.join(DOCS_PROTOCOL_BASE_DIR, "terminology.md")
DOCS_MEMORY_LIFECYCLE  = os.path.join(DOCS_PROTOCOL_BASE_DIR, "memory_lifecycle.md")
DOCS_SCHEMA            = os.path.join(DOCS_PROTOCOL_BASE_DIR, "schema.md")
DOCS_POPUP_TEMPLATE    = os.path.join(DOCS_PROTOCOL_BASE_DIR, "popup.md")
DOCS_PROTOCOL_TOOLS    = os.path.join(DOCS_PROTOCOL_BASE_DIR, "tools.md")

# ============================================================
# persona/STM/
# ============================================================

STM_DIR         = os.path.join(PERSONA_DIR, "STM")
STM_MEMORY_DIR  = os.path.join(STM_DIR, "memory")
STM_BUFFER_DIR  = os.path.join(STM_DIR, "buffer")
STM_HEALTH_DIR  = os.path.join(STM_DIR, "health", "base")
STM_CONTEXT_DIR = os.path.join(STM_DIR, "context")

# STM/memory/ — 记忆区
MEMORY_MD      = os.path.join(STM_MEMORY_DIR, "memory.md")
INDEX_MD       = os.path.join(STM_MEMORY_DIR, "index.md")
KEYWORDS_JSON  = os.path.join(STM_MEMORY_DIR, "keywords.json")
HEAT_JSON      = os.path.join(STM_MEMORY_DIR, "heat.json")
META_JSON      = os.path.join(STM_MEMORY_DIR, "meta.json")
DREAMS_MD      = os.path.join(STM_MEMORY_DIR, "dreams.md")

# V2: 倒排索引独立目录（DDS §26）
INVERTED_INDEX_DIR    = os.path.join(STM_MEMORY_DIR, "inverted")
INVERTED_INDEX_STM    = os.path.join(INVERTED_INDEX_DIR, "stm_index.json")
INVERTED_INDEX_LTM    = os.path.join(INVERTED_INDEX_DIR, "ltm_index.json")
INVERTED_INDEX_SKILLS = os.path.join(INVERTED_INDEX_DIR, "skills_index.json")
INVERTED_INDEX_RELATION = os.path.join(INVERTED_INDEX_DIR, "relation_index.json")

# V2: 节志目录（DDS §27）
JOURNAL_DIR = os.path.join(STM_MEMORY_DIR, "journal")

# STM/context/ — 内容窗口清单状态
RESIDENT_LIST_JSON = os.path.join(STM_CONTEXT_DIR, "resident_list.json")

# STM/buffer/ — 缓冲区
RAW_LOG_JSONL      = os.path.join(STM_BUFFER_DIR, "raw_log.jsonl")
RAW_LOG            = os.path.join(STM_BUFFER_DIR, "raw_log.md")
INTERRUPTS_JSONL   = os.path.join(STM_BUFFER_DIR, "interrupts.jsonl")
STATE_BACKUPS_JSONL = os.path.join(STM_BUFFER_DIR, "state_backups.jsonl")
STATE_SETTLEMENT_JOURNAL_JSON = os.path.join(
    STM_BUFFER_DIR, "state_settlement_journal.json")

# STM/health/base/ — 健康监控
CONNECTIVITY_JSON = os.path.join(STM_HEALTH_DIR, "connectivity.json")
ALERTS_MD         = os.path.join(STM_HEALTH_DIR, "alerts.md")
WEB_BACKEND_HEALTH_JSON = os.path.join(STM_HEALTH_DIR, "web_backend_health.json")

# STM/context/ — 上下文审计痕迹
STM_CTX_SETUP_DIR    = os.path.join(STM_CONTEXT_DIR, "setup")
STM_CTX_REACTION_DIR = os.path.join(STM_CONTEXT_DIR, "reaction")
STM_CTX_CLEANUP_DIR  = os.path.join(STM_CONTEXT_DIR, "cleanup")
STM_CTX_ROUND_DIR    = os.path.join(STM_CONTEXT_DIR, "round")
STM_CONTEXT_CACHE_DIR = os.path.join(STM_CONTEXT_DIR, "cache")
STM_CONTEXT_NOW_CACHE_JSONL = os.path.join(STM_CONTEXT_CACHE_DIR, "now_cache.jsonl")
STM_CONTEXT_LATELY_CACHE_JSONL = os.path.join(STM_CONTEXT_CACHE_DIR, "lately_cache.jsonl")

# STM/workbench/ — WB 调度台（DDS §34）
WB_DIR        = os.path.join(STM_DIR, "workbench")
WB_STATUS_JSON = os.path.join(WB_DIR, "status.json")
WB_INPUT_DIR  = os.path.join(WB_DIR, "input")
WB_PROCESS_DIR = os.path.join(WB_DIR, "process")
WB_OUTPUT_DIR = os.path.join(WB_DIR, "output")
# WB_MANIFEST_JSON — 已淘汰，面单在各任务子目录内

# ============================================================
# persona/LTM/
# ============================================================

LTM_DIR        = os.path.join(PERSONA_DIR, "LTM")
LTM_MEMORY_DIR = os.path.join(LTM_DIR, "Memory")

# LTM/Memory/ 四层 + Pinned
LTM_FULL_DIR     = os.path.join(LTM_MEMORY_DIR, "Full")
LTM_SUMMARY_DIR  = os.path.join(LTM_MEMORY_DIR, "Summary")
LTM_ABSTRACT_DIR = os.path.join(LTM_MEMORY_DIR, "Abstract")
LTM_BACKUP_DIR   = os.path.join(LTM_MEMORY_DIR, "Backup")
LTM_PINNED_DIR   = os.path.join(LTM_MEMORY_DIR, "Pinned")   # DDS §4.11

# LTM/Memory/ 各层文件
LTM_FULL_FULL_MD         = os.path.join(LTM_FULL_DIR, "full.md")
LTM_FULL_INDEX_MD        = os.path.join(LTM_FULL_DIR, "index.md")
LTM_FULL_META_JSON       = os.path.join(LTM_FULL_DIR, "meta.json")
LTM_SUMMARY_SUMMARY_MD   = os.path.join(LTM_SUMMARY_DIR, "summary.md")
LTM_SUMMARY_INDEX_MD     = os.path.join(LTM_SUMMARY_DIR, "index.md")
LTM_SUMMARY_META_JSON    = os.path.join(LTM_SUMMARY_DIR, "meta.json")
LTM_ABSTRACT_ABSTRACT_MD = os.path.join(LTM_ABSTRACT_DIR, "abstract.md")
LTM_ABSTRACT_INDEX_MD    = os.path.join(LTM_ABSTRACT_DIR, "index.md")
LTM_ABSTRACT_META_JSON   = os.path.join(LTM_ABSTRACT_DIR, "meta.json")
LTM_ABSTRACT_FUZZY_DREAMS_MD = os.path.join(LTM_ABSTRACT_DIR, "fuzzy_dreams.md")
LTM_BACKUP_BACKUP_MD     = os.path.join(LTM_BACKUP_DIR, "backup.md")
LTM_BACKUP_INDEX_MD      = os.path.join(LTM_BACKUP_DIR, "index.md")
LTM_BACKUP_META_JSON     = os.path.join(LTM_BACKUP_DIR, "meta.json")
LTM_PINNED_PINNED_MD     = os.path.join(LTM_PINNED_DIR, "pinned.md")
LTM_PINNED_META_JSON     = os.path.join(LTM_PINNED_DIR, "meta.json")

LTM_KEYWORDS_JSON = os.path.join(LTM_MEMORY_DIR, "keywords.json")

# LTM/ 9种工作容器目录（DDS §13-18）
CONTAINER_DIALECTICS_DIR = os.path.join(LTM_DIR, "Dialectics")  # DC-
CONTAINER_EVENTS_DIR     = os.path.join(LTM_DIR, "Events")      # EC-
CONTAINER_PROJECTS_DIR   = os.path.join(LTM_DIR, "Projects")    # PRJ-
CONTAINER_SKILLS_DIR     = os.path.join(LTM_DIR, "Skills")      # SKL-
CONTAINER_IMMUNE_DIR     = os.path.join(LTM_DIR, "Immune")      # IMM-
CONTAINER_CHRONICLE_DIR  = os.path.join(LTM_DIR, "Chronicle")   # CHR-
CONTAINER_CORPUS_DIR     = os.path.join(LTM_DIR, "Corpus")      # COR-
CONTAINER_FUTURE_DIR     = os.path.join(LTM_DIR, "Future")      # FUT-
CONTAINER_ITERATION_DIR  = os.path.join(LTM_DIR, "Iteration")   # ITR-

# container_registry（WB不进此表）
CONTAINER_REGISTRY_JSON = os.path.join(LTM_DIR, "container_registry.json")

# LTM/Chronicle/ 节志
CHRONICLE_RHYTHMS_DIR = os.path.join(CONTAINER_CHRONICLE_DIR, "rhythms")

# ============================================================
# persona/relation/ — 关系域
# ============================================================

RELATION_DIR           = os.path.join(PERSONA_DIR, "relation")
RELATION_REGISTRY_JSON = os.path.join(RELATION_DIR, "relation_registry.json")
RELATION_INDEX_DIR     = os.path.join(RELATION_DIR, "_index")
RELATION_KEYWORDS_JSON = os.path.join(RELATION_INDEX_DIR, "keywords.json")

# ============================================================
# config/ — OS 配置文件（DDS §32）
# ============================================================

CONFIG_SYSTEM   = os.path.join(CONFIG_DIR, "system.json")
LEGACY_CONFIG_API = os.path.join(CONFIG_DIR, "api.json")
CONFIG_MEMORY   = os.path.join(CONFIG_DIR, "memory.json")
CONFIG_MEDIA    = os.path.join(CONFIG_DIR, "media.json")
CONFIG_RELATION = os.path.join(CONFIG_DIR, "relation.json")
CONFIG_MODEL_ROUTING = os.path.join(CONFIG_DIR, "model_routing.json")

# UPSP/config/ — 跨位格全局本机配置
GLOBAL_INTERFACE_CONFIG = os.path.join(GLOBAL_CONFIG_DIR, "interface.json")
GLOBAL_MODELS_CONFIG = os.path.join(GLOBAL_CONFIG_DIR, "models.json")

# config/context/ — 上下文装配规则（DDS §32）
CONTEXT_CONFIG_DIR     = os.path.join(CONFIG_DIR, "context")
CONTEXT_PERMANENT_JSON = os.path.join(CONTEXT_CONFIG_DIR, "permanent.json")
CONTEXT_PERIODIC_JSON  = os.path.join(CONTEXT_CONFIG_DIR, "periodic.json")
CONTEXT_HIGH_FREQ_JSON = os.path.join(CONTEXT_CONFIG_DIR, "high_freq.json")
CONTEXT_NOW_JSON      = os.path.join(CONTEXT_CONFIG_DIR, "now.json")
CONTEXT_LATELY_JSON   = os.path.join(CONTEXT_CONFIG_DIR, "lately.json")
CONTEXT_STATUSBAR_JSON = os.path.join(CONTEXT_CONFIG_DIR, "statusbar.json")
CONTEXT_POPUP_JSON     = os.path.join(CONTEXT_CONFIG_DIR, "popup.json")

# 运行时POPUP数据（DDS §24：属于STM运行时，非配置文件）
CONTEXT_POPUP          = os.path.join(STM_CONTEXT_DIR, "popup.md")

# ============================================================
# 训练材料四集（DDS §31）
# ============================================================

# 训练材料（DDS §31：存放在 LTM/Iteration/ 下）
TRAINING_RAW_DIR       = os.path.join(CONTAINER_ITERATION_DIR, "Raw")
TRAINING_MATERIALS_DIR = os.path.join(CONTAINER_ITERATION_DIR, "Materials")
TRAINING_DIR           = TRAINING_RAW_DIR                   # 向后兼容
TACIT_SET_DIR          = os.path.join(TRAINING_RAW_DIR, "Tacit")
ASSOCIATION_SET_DIR    = os.path.join(TRAINING_RAW_DIR, "Association")
CONNECTION_SET_DIR     = os.path.join(TRAINING_RAW_DIR, "Connection")
EVOLUTION_SET_DIR      = os.path.join(TRAINING_MATERIALS_DIR, "Evolution")

# ============================================================
# trash/ 垃圾桶子结构（DDS 衰减期保留）
# ============================================================

TRASH_DC       = os.path.join(TRASH_DIR, "DC.md")
TRASH_EC       = os.path.join(TRASH_DIR, "EC.md")
TRASH_PRJ      = os.path.join(TRASH_DIR, "PRJ.md")
TRASH_SKL      = os.path.join(TRASH_DIR, "SKL.md")
TRASH_IMM      = os.path.join(TRASH_DIR, "IMM.md")
TRASH_FUT      = os.path.join(TRASH_DIR, "FUT.md")
TRASH_NOTES    = os.path.join(TRASH_DIR, "notes.md")
TRASH_RELATION = os.path.join(TRASH_DIR, "relation.md")

# ============================================================


# ============================================================
# 路径完整性自检
# ============================================================

_ALL_PATHS = None  # 缓存

def list_all_paths():
    """返回全部已定义路径 → {变量名: 路径}，供测试验证"""
    global _ALL_PATHS
    if _ALL_PATHS is None:
        import inspect
        _ALL_PATHS = {}
        module = inspect.getmodule(inspect.currentframe())
        for name in dir(module):
            if name.isupper() and not name.startswith("_"):
                val = getattr(module, name)
                if isinstance(val, str) and ("\\" in val or "/" in val):
                    _ALL_PATHS[name] = val
    return _ALL_PATHS


if __name__ == "__main__":
    print(f"OS_ROOT: {OS_ROOT}")
    print(f"PERSONA_DIR: {PERSONA_DIR}  存在={os.path.isdir(PERSONA_DIR)}")
    print(f"路径总数: {len(list_all_paths())}")
    for name, path in sorted(list_all_paths().items()):
        exists = os.path.exists(path)
        suffix = "" if exists else " [!不存在]"
        print(f"  {name}: {path}{suffix}")
