import type { Locale, MessageKey } from "./i18n";

export type PageId =
  | "run"
  | "persona"
  | "mem"
  | "relations"
  | "containers"
  | "context"
  | "audit"
  | "settings";

export type DepositionKind = "memory" | "container" | "relation";
export type PermissionLevel = "limited" | "guarded" | "unlimited";
export type GlobalSettingsTab = "models" | "interface" | "about";
export type JsonObject = Record<string, unknown>;
export type SettingsFileId =
  | "system"
  | "memory"
  | "now"
  | "lately"
  | "periodic"
  | "high_freq"
  | "relation"
  | "interface"
  | "models"
  | "model_routing";
export type SettingValue = string | number | boolean | null | JsonObject | unknown[];

export interface ConsolePage {
  id: PageId;
  code: string;
  title: MessageKey;
  icon: string;
  manual: string;
  nav?: boolean;
}

export interface PageTab {
  id: string;
  label: MessageKey;
  code: string;
}

export interface Shortcut {
  id: string;
  label: string;
  name: MessageKey;
  target: PageId;
  tab: string;
  icon: string;
}

export type ProviderErrorKind =
  | "cancelled"
  | "connection_refused"
  | "dns_error"
  | "tls_error"
  | "timeout"
  | "connection_interrupted"
  | "authentication"
  | "permission_denied"
  | "model_unavailable"
  | "rate_limit_or_quota"
  | "context_too_long"
  | "endpoint_not_found"
  | "request_rejected"
  | "upstream_unavailable"
  | "invalid_response"
  | "service_error"
  | "unknown";

export interface ProviderErrorHint {
  kind: ProviderErrorKind;
  http_statuses: number[];
  target?: string;
}

export interface ConversationCard {
  approval_id?: string;
  decision?: "allow_once" | "skip" | "cancelled";
  tool_id?: string;
  tool_signature?: string;
  card_id?: string;
  content?: unknown;
  content_md?: unknown;
  content_raw?: unknown;
  default_collapsed?: boolean;
  event_index?: number;
  event_type: string;
  frame_id?: string;
  iteration?: number | null;
  phase?: string;
  provider_error_hint?: ProviderErrorHint;
  recorded_at?: string;
  severity?: string;
  stream_id?: string;
  stream_state?: "active" | "completed" | "interrupted" | "stopped";
  summary?: string;
  title?: string;
  type: string;
}

export interface CallFrame {
  round?: number;
  frame_id: string;
  label?: string;
  phase?: string;
  iteration?: number | null;
  call_channel?: string;
  model_profile_id?: string;
  created_at?: string;
  layer_source?: string;
  event_start_index?: number | null;
  event_end_index?: number | null;
  historical?: boolean;
  context_panes?: ContextPane[];
  context_usage?: {
    chars: number;
    input_tokens: number | null;
    window_tokens: number | null;
    state: "pending" | "reported" | "unavailable";
  };
  manifest?: JsonObject;
}

export interface ContextPane {
  id: string;
  title: string;
  chars?: number;
  raw_chars?: number;
  content_md?: string;
  content_raw?: string;
  content_blocks?: ContextContentBlock[];
}

export interface ContextContentBlock {
  block_id: string;
  title?: string;
  chars?: number;
  raw_chars?: number;
  content_md?: string;
  content_raw?: string;
  tone?: string;
  source_block_id?: string;
  provenance?: JsonObject;
}

export interface RequestPrefixDiffTarget {
  pane_id: string;
  block_id: string;
  placement: "layer_start" | "layer_inside" | "layer_boundary" | "block_inside" | "block_boundary" | "layer_end" | "request_end";
  change_kind: "insert" | "delete" | "replace";
  source_offset: number;
  block_offset?: number;
  request_path: string;
  source_mapping?: "identity" | "derived";
}

export interface RequestPrefixDiffPayload {
  schema_version: "seed_gui_request_prefix_diff.v1";
  state: "ready" | "identical" | "unavailable";
  reason?: string;
  current?: { round: number; frame_id: string; wire_body_sha256: string };
  previous?: { round: number; frame_id: string; wire_body_sha256: string };
  common_prefix_bytes?: number;
  current_wire_bytes?: number;
  previous_wire_bytes?: number;
  prefix_ratio?: number;
  changed_suffix_bytes?: number;
  target?: RequestPrefixDiffTarget;
}

export interface RoundLifecycle {
  state?: string;
  settlement_status?: string;
  fatal_reasons?: string[];
  degraded_reasons?: string[];
  event_indexes?: Record<string, number>;
}

export interface StatusbarProjection {
  schema?: string;
  mode?: string;
  dynamic?: string;
  workhood?: string;
  round?: {
    id?: string;
    type?: string;
    progress?: string;
  };
}

export interface LiveState {
  schema_version: "round_live_state.v2";
  round?: number;
  last_event_index?: number;
  latest_frame_id?: string;
  call_frames?: CallFrame[];
  context_panes?: ContextPane[];
  conversation?: ConversationCard[];
  manifest?: JsonObject;
  statusbar_projection?: StatusbarProjection | null;
  round_lifecycle?: RoundLifecycle | null;
}

export interface RuntimeCliData {
  round_type?: string;
  active_flags?: string[];
  active_guides?: Record<string, string>;
}

export interface RuntimeStatus {
  schema_version: "seed_gui_runtime_status.v2";
  host_session?: string;
  process_id?: number;
  current_round?: number | null;
  round_type?: string | null;
  stage?: string;
  stop_requested?: boolean;
  can_stop?: boolean;
  heartbeat_suspended?: boolean;
  last_outcome?: JsonObject;
  send_in_flight?: boolean;
  relay_in_flight?: boolean;
  mutation_in_flight?: boolean;
  restart_requested?: boolean;
  pending_tool_approval?: {
    schema_version: "general_tool_approval.v1";
    approval_id: string;
    round?: number;
    frame_id?: string;
    iteration?: number;
    tool_id: string;
    tool_label?: string;
    tool_signature?: string;
    summary?: string;
    requested_at?: string;
    details?: JsonObject;
  } | null;
  execution_permission?: {
    permission_level?: PermissionLevel;
    permission_label?: string;
    pending_level?: PermissionLevel | null;
    requested_at?: string | null;
  };
  cli?: {
    ok?: boolean;
    command?: string;
    data?: RuntimeCliData;
  };
}

export interface RoundListPayload {
  rounds: Array<{ round: number }>;
}

export interface LivePayload {
  round?: number | null;
  state?: LiveState | null;
}

export interface LiveEventsPayload extends LivePayload {
  schema_version: "round_live_events.v1";
}

export interface TaskRecord {
  id: string;
  title?: string;
  summary?: string;
  status?: string;
  required?: boolean;
  reason?: string;
  evidence_refs?: string[];
}

export interface TaskPendingInput {
  id: string;
  status?: string;
  summary?: string;
  source_refs?: string[];
}

export interface WorkbenchTask {
  id?: string;
  title?: string;
  goal?: string;
  pending_inputs?: TaskPendingInput[];
  source_requirements?: TaskRecord[];
  risk_notes?: string[];
  items?: TaskRecord[];
  acceptance?: TaskRecord[];
}

export interface TaskProjectionPayload {
  schema_version: "seed_gui_task_projection.v1";
  task: WorkbenchTask | null;
  active_guides?: { work?: string };
  summary?: {
    open_items?: number;
    pending_acceptance?: number;
    open_pending_inputs?: number;
    state?: string;
  };
}

export interface ProtocolCatalogUse {
  category: string;
  target?: string;
  trigger?: string;
  usage?: string;
  tier?: string;
  source_mode?: string;
  consume?: string;
}

export interface ProtocolCatalogEntry {
  id: string;
  kind: "rule" | "doc";
  file: string;
  path: string;
  source_ref: string;
  description?: string;
  category?: string;
  categories?: string[];
  layer?: string;
  scope?: string;
  load?: string;
  trigger?: string;
  uses?: ProtocolCatalogUse[];
}

export interface ProtocolRuleCategory {
  id: string;
  count: number;
  entries: ProtocolCatalogEntry[];
}

export interface ProtocolCatalogPayload {
  schema_version: "seed_gui_protocol_catalog.v1";
  rules: {
    registry_version: string;
    registry_note: string;
    total: number;
    categories: ProtocolRuleCategory[];
  };
  docs: {
    registry_version: string;
    registry_note: string;
    registrations: number;
    total: number;
    entries: ProtocolCatalogEntry[];
  };
}

export interface ProtocolDocumentPayload {
  schema_version: "seed_gui_protocol_document.v1";
  kind: "rule" | "doc";
  id: string;
  title: string;
  description: string;
  source_ref: string;
  categories: string[];
  content_md: string;
}

export interface ProtocolProjection {
  catalog: ProtocolCatalogPayload | null;
  loading: boolean;
  error: string;
  renderKey: string;
}

export interface PersonaCorePayload {
  schema_version: "seed_gui_persona_core.v1";
  source_ref: string;
  content_md: string;
}

export interface PersonaStateField {
  path: string;
  value: unknown;
}

export interface PersonaStatePayload {
  schema_version: "seed_gui_persona_state.v1";
  observed_at: string;
  source_ref: string;
  dynamic_descriptions: Record<string, string>;
  fields: PersonaStateField[];
}

export interface PersonaProjection {
  core: PersonaCorePayload | null;
  state: PersonaStatePayload | null;
  coreLoading: boolean;
  stateLoading: boolean;
  coreError: string;
  stateError: string;
  stateStale: boolean;
  coreRenderKey: string;
  stateRenderKey: string;
}

export interface ContainerEntry {
  mem_id?: string;
  title?: string;
  target_file?: string;
  round?: number | null;
}

export interface DepositionItem {
  id: string;
  title?: string;
  name?: string;
  status?: string;
  subject?: string;
  weight?: number;
  tags?: string[];
  source_refs?: string[];
  memory_layer?: string;
  memory_layers?: string[];
  stm_present?: boolean;
  ltm_layer?: string;
  periodic_mounted?: boolean;
  periodic_pin_owned?: boolean;
  periodic_mount_status?: string;
  periodic_mount_reason?: string;
  linked_containers?: string[];
  prefix?: string;
  type?: string;
  current_overview?: string;
  current_overview_updated_at?: string;
  focus?: boolean;
  category?: string;
  created_round?: number | null;
  created_instance_id?: string;
  last_recalled_round?: number | null;
  last_recalled_instance_id?: string;
  created_at?: string;
  stored_at?: string;
  admission_status?: string;
  last_recalled_at?: string;
  entries?: ContainerEntry[];
}

export interface RelationNote {
  date?: string;
  content: string;
}

export interface DepositionDetailItem extends DepositionItem {
  body?: string;
  content?: string;
  content_truncated?: boolean;
  default_target?: string;
  axes?: Record<string, number>;
  notes?: RelationNote[];
}

export interface DepositionDetailPayload {
  schema_version: "seed_gui_deposition_detail.v1";
  kind: DepositionKind;
  item: DepositionDetailItem;
}

export interface ContainerFocusReceipt {
  tool_id?: string;
  status?: string;
  action?: string;
  container_id?: string;
  reason?: string;
}

export interface DepositionIndexPayload {
  schema_version: "seed_gui_deposition_index.v1";
  memory: DepositionItem[];
  containers: DepositionItem[];
  relations: DepositionItem[];
  focus?: { current?: string; previous?: string };
}

export interface RuntimeProjection {
  host: "connecting" | "connected" | "error";
  status: RuntimeStatus | null;
  live: LiveState | null;
  round: number | null;
  error: string;
  sendFeedback: string;
  exportFeedback: string;
  sending: boolean;
  stopping: boolean;
  awaitingProjection: boolean;
  submitBaseline: { round: number | null; eventIndex: number } | null;
  unlimitedConfirmed: boolean;
  permissionChanging: boolean;
  approvalSubmitting: string;
  approvalFeedback: string;
  fullRefreshNeeded: boolean;
  renderKey: string;
  conversationRounds: Map<number, LiveState>;
  conversationRoundOrder: number[];
  conversationHistoryInitialized: boolean;
  conversationHistoryLatest: number | null;
  conversationHistoryError: string;
  conversationHistoryVersion: number;
  contextPrefixDiff: RequestPrefixDiffPayload | null;
  contextPrefixDiffKey: string;
  contextPrefixDiffLoading: boolean;
  contextPrefixDiffError: string;
}

export interface PeriodicMemoryResidence {
  memory_layers?: string[];
  stm_present?: boolean;
  ltm_layer?: string;
  periodic_mounted?: boolean;
  periodic_pin_owned?: boolean;
  pin_source?: string;
  periodic_mount_status?: string;
  periodic_mount_reason?: string;
}

export interface PeriodicMemoryMountReceipt {
  schema_version: "periodic_memory_mount_receipt.v2";
  tool_id: "periodic_memory_mount";
  receipt_id?: string;
  status: "applied" | "noop";
  action: "mount" | "unmount";
  mem_id: string;
  instance_id?: string;
  before?: PeriodicMemoryResidence;
  after?: PeriodicMemoryResidence;
  owners_before?: string[];
  owners_after?: string[];
  periodic_chars_before?: number;
  periodic_chars_after?: number;
  periodic_chars_limit?: number;
  cache_invalidated?: boolean;
  provider_called?: boolean;
  recall_applied?: boolean;
  recorded_at?: string;
  mount_status?: string;
  outcome?: string;
  pending_reason?: string;
}

export interface DepositionProjection {
  index: DepositionIndexPayload | null;
  details: Record<DepositionKind, Record<string, DepositionDetailPayload>>;
  pendingDetails: Set<string>;
  detailErrors: Record<string, string>;
  focusMutation: {
    pending: boolean;
    feedback: string;
    receipt: ContainerFocusReceipt | null;
  };
  periodicMutation: {
    pending: boolean;
    memId: string;
    feedback: string;
    receipt: PeriodicMemoryMountReceipt | null;
  };
  loading: boolean;
  error: string;
  renderKey: string;
}

export interface TaskProjection {
  data: TaskProjectionPayload | null;
  loading: boolean;
  error: string;
  relayPending: boolean;
  relayFeedback: string;
  renderKey: string;
}

export interface SettingsPayload {
  schema_version: "seed_gui_settings.v3";
  environment_override: boolean;
  files: Record<string, {
    revision: string;
    values: Record<string, SettingValue>;
  }>;
  interface: {
    revision: string;
    values: { schema_version: string; locale: "system" | Locale };
  };
  model_catalog: {
    revision: string;
    connections: ModelConnection[];
    models: ModelProfile[];
    transport: JsonObject;
    key_sources: Record<string, string>;
  };
  persona: {
    model_routing: {
      revision: string;
      values: ModelRoutingConfig;
    };
    effective_routes: EffectiveModelRoutes;
    setup_model_ready: boolean;
  };
  ready: boolean;
}

export interface ModelConnection {
  id: string;
  alias: string;
  protocol: "openai_chat" | "openai_responses" | "anthropic_messages";
  url: string;
  api_key_env: string;
  key_source: "env" | "config" | "missing" | string;
  key_present: boolean;
}

export interface ModelReasoningConfig {
  supported: string[];
  default: string;
}

export interface ModelProfile {
  id: string;
  alias: string;
  connection_id: string;
  model: string;
  context_window: number;
  output_token_limit: number;
  detected_context_window?: number;
  context_window_source?: "provider" | "registry" | "legacy_manual" | "unknown";
  reasoning: ModelReasoningConfig;
  streaming: { enabled?: boolean; protocol?: string; include_usage?: boolean };
  prompt_cache: { profile?: string };
  request_overrides: JsonObject;
}

export interface ModelRouteSlot {
  model_id: string;
  reasoning_effort: string;
}

export interface ModelRoutingConfig {
  schema_version: string;
  cross_phase_failover_enabled: boolean;
  routes: Record<"setup" | "reaction" | "cleanup", {
    primary: ModelRouteSlot | null;
    backups: [ModelRouteSlot | null, ModelRouteSlot | null];
  }>;
}

export interface EffectiveModelRoute {
  model_id: string;
  model_alias: string;
  connection_id: string;
  connection_alias: string;
  reasoning_effort: string;
  source_phase: string;
  slot: string;
  inherited: boolean;
}

export interface EffectiveModelRoutes {
  cross_phase_failover_enabled: boolean;
  effective_primaries: Record<string, ModelRouteSlot | null>;
  primary_sources: Record<string, string | null>;
  phases: Record<"setup" | "reaction" | "cleanup", EffectiveModelRoute[]>;
}

export interface SettingsProjection {
  data: SettingsPayload | null;
  loading: boolean;
  pending: boolean;
  error: string;
  feedback: string;
  renderKey: string;
}

export interface ModelContextResolution {
  schema_version: "seed_gui_model_context_resolution.v1";
  model: string;
  detected_context_window: number | null;
  source: "provider" | "registry" | "unknown";
  source_ref?: string;
}

export interface AboutPayload {
  schema_version: "seed_gui_about.v1";
  product: {
    name: string;
    version: string;
    channel: string;
    build_number: number;
    author: Record<Locale, string>;
    license: string;
    copyright: string;
  };
  links: { repository: string; releases: string };
  build: {
    git_head: string;
    source_dirty: boolean;
    architecture: string;
    signature_status: string;
  };
  data_policy: {
    persona_location: string;
    local_state_location: string;
    uninstall_preserves_user_data: boolean;
    key_storage: string;
  };
}

export interface AboutProjection {
  data: AboutPayload | null;
  loading: boolean;
  error: string;
}

export interface BootstrapIdentity {
  pid: string;
  name_zh: string;
  name_en: string;
  abbreviation: string;
  display_name: string;
}

export type PersonaNameVariant = "name_zh" | "name_en" | "abbreviation";

export interface BootstrapPersonaPreset {
  id: string;
  name_zh: string;
  name_en: string;
  abbreviation: string;
  roles: string[];
  axes: Record<"S" | "C" | "V" | "A" | "R" | "B", number>;
  persona_code: string;
  traits: string[];
  self_description: string;
  instance_notes: string;
}

export interface BootstrapStatusPayload {
  schema_version: "seed_gui_bootstrap_status.v1";
  persona: {
    state: "missing" | "incomplete" | "config_error" | "ready";
    ready: boolean;
    missing: string[];
    config_error?: {
      code: "persona_config_migration_failed";
      config: string;
      path: string;
      reason: string;
    };
  };
  identity: BootstrapIdentity | null;
  preset: BootstrapPersonaPreset | null;
  setup_primary: {
    profile_id: string;
    model_alias: string;
    model: string;
    connection_alias: string;
    context_window: number;
    reasoning_effort: string;
  } | null;
  setup_error: string;
  provider_test: {
    valid: boolean;
    ttl_seconds: number;
  };
}

export interface BootstrapDraft {
  name_zh: string;
  name_en: string;
  abbreviation: string;
  roles: [string, string, string];
  axes: Record<"S" | "C" | "V" | "A" | "R" | "B", number>;
  self_description: string;
  traits: [string, string, string];
  instance_notes: string;
}

export interface BootstrapProjection {
  data: BootstrapStatusPayload | null;
  loading: boolean;
  pending: boolean;
  error: string;
  feedback: string;
  renderKey: string;
  selection: "choice" | "preset" | "custom";
  preview: boolean;
  testToken: string;
  skipModelSetup: boolean;
  manageNewPersona: boolean;
  draft: BootstrapDraft;
}

export interface InstanceCatalogItem {
  instance_id: string;
  kind: "meta" | "branch";
  label: string;
  source_instance_id: string;
  source_round: number;
  created_at: string;
  archived: boolean;
}

export interface PersonaCatalogPayload {
  schema_version: "seed_gui_persona_catalog.v1";
  active: { pid: string; instance_id: string };
  personas: Array<{
    pid: string;
    identity: Partial<BootstrapIdentity>;
    instances: InstanceCatalogItem[];
  }>;
}

export interface PersonaCatalogProjection {
  data: PersonaCatalogPayload | null;
  loading: boolean;
  pending: boolean;
  error: string;
}

export interface PollingState {
  about: Promise<boolean> | null;
  bootstrap: Promise<boolean> | null;
  personas: Promise<boolean> | null;
  runtime: Promise<boolean> | null;
  runtimeForceQueued: boolean;
  task: Promise<boolean> | null;
  taskForceQueued: boolean;
  deposition: Promise<boolean> | null;
  depositionForceQueued: boolean;
  settings: Promise<boolean> | null;
  settingsForceQueued: boolean;
  personaCore: Promise<boolean> | null;
  personaState: Promise<boolean> | null;
}

export interface UiState {
  locale: Locale;
  activePage: PageId;
  activeTabs: Record<PageId, string>;
  manualFile: string;
  overviewCollapsed: boolean;
  overviewSectionsCollapsed: Set<string>;
  conversationDisclosure: Map<string, boolean>;
  navCollapseLocked: boolean;
  systemWindowOpen: boolean;
  globalSettingsOpen: boolean;
  globalSettingsTab: GlobalSettingsTab;
  editingConnectionId: string | null;
  editingModelId: string | null;
  selectedMemoryId: string;
  selectedContainerId: string;
  selectedRelationId: string;
  memoryQuery: string;
  activeRuntimePane: string;
  selectedTaskRound: number | null;
  selectedTaskFrame: string | null;
  selectedContextRound: number | null;
  selectedContextFrame: string | null;
  selectedLedgerRound: number | null;
}

export interface Elements {
  bootstrapRoot: HTMLElement;
  app: HTMLElement;
  leftRail: HTMLElement;
  personaTabs: HTMLElement;
  personaMoreMenu: HTMLDetailsElement;
  personaMoreToggle: HTMLElement;
  personaNameOptions: HTMLElement;
  createPersonaButton: HTMLButtonElement;
  instanceTabs: HTMLElement;
  instanceMoreMenu: HTMLDetailsElement;
  instanceMoreToggle: HTMLElement;
  instanceOptions: HTMLElement;
  createInstanceButton: HTMLButtonElement;
  identityFeedback: HTMLElement;
  statusReadouts: HTMLElement;
  productVersionName: HTMLElement;
  productVersionNumber: HTMLElement;
  surfaceNav: HTMLElement;
  navLockToggle: HTMLButtonElement;
  pageCode: HTMLElement;
  pageTitle: HTMLElement;
  stagePage: HTMLElement;
  overviewContent: HTMLElement;
  overviewPane: HTMLElement;
  chatThread: HTMLElement;
  overviewToggle: HTMLButtonElement;
  globalSettingsToggle: HTMLButtonElement;
  globalSettingsOverlay: HTMLElement;
  globalSettingsContent: HTMLElement;
  manualOverlay: HTMLElement;
  manualTitle: HTMLElement;
  manualSummary: HTMLElement;
  manualPageLabel: HTMLElement;
  manualSources: HTMLElement;
  manualBody: HTMLElement;
  runtimeState: HTMLElement;
  commsSource: HTMLElement;
  runtimeComposer: HTMLFormElement;
  messageInput: HTMLTextAreaElement;
  permissionLevel: HTMLSelectElement;
  sendButton: HTMLButtonElement;
  stopButton: HTMLButtonElement;
  configureModelButton: HTMLButtonElement;
  sendFeedback: HTMLElement;
  contextUsage: HTMLOutputElement;
  ledgerRound: HTMLElement;
  ledgerContext: HTMLElement;
  ledgerFrame: HTMLElement;
  ledgerSettlement: HTMLElement;
}

export interface FocusReturnDescriptor {
  pageId: PageId;
  tabId: string;
}

export type SystemReturnFocus = HTMLElement | FocusReturnDescriptor | null;

export interface RelayRuntimeState {
  ready: boolean;
  inFlight: boolean;
  mutationInFlight: boolean;
  roundType: string;
  activeFlags: string[];
}
