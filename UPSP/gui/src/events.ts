import type {
  DepositionKind,
  JsonObject,
  PageId,
  SettingValue,
  SettingsFileId,
  SystemReturnFocus,
} from "./contracts";
import { t } from "./i18n";
import { initMarkdownInteractions } from "./markdown";
import { els, runtimeProjection, settingsProjection, state } from "./state";
import {
  depositionPage,
  exportCurrentEvidence,
  loadActiveDepositionDetail,
  loadDepositionDetail,
  pollAbout,
  pollDeposition,
  pollPersonaCore,
  pollPersonaProjection,
  pollPersonaState,
  pollProtocolCatalog,
  pollRuntime,
  pollSettings,
  pollTaskProjection,
  refreshRuntimeUi,
  retryProjection,
  submitContainerFocus,
  submitRuntimeMessage,
  submitRuntimeRelay,
  submitRuntimeStop,
  submitProviderKey,
  submitModelCatalog,
  submitSettings,
} from "./runtime";
import {
  aboutDiagnosticText,
  closeManual,
  closeGlobalSettings,
  closeSystemWindow,
  collapseNavNow,
  getActivePageTab,
  openManual,
  openGlobalSettings,
  openMemoryDetail,
  openContextToolAnnotation,
  openLedgerEvent,
  openNav,
  openProtocolDocument,
  rememberSystemReturnFocus,
  renderNavigation,
  renderOverview,
  renderGlobalSettings,
  renderIdentity,
  renderStage,
  renderStageAndFocus,
  scheduleNavCollapse,
  setActivePageTab,
  setPage,
  selectPersonaNameVariant,
  syncNavPointer,
  syncShellState,
} from "./view";

function eventElement(event: Event): Element | null {
  return event.target instanceof Element ? event.target : null;
}

function settingValues(root: ParentNode): Record<string, SettingValue> {
  const values: Record<string, SettingValue> = {};
  root.querySelectorAll<HTMLInputElement | HTMLSelectElement>("[data-setting-key]").forEach((input) => {
    const key = input.dataset.settingKey;
    if (!key) return;
    const kind = input.dataset.settingKind;
    if (input instanceof HTMLInputElement && input.type === "checkbox") {
      values[key] = input.checked;
    } else if (kind === "int") {
      values[key] = Number.parseInt(input.value, 10);
    } else if (kind === "float") {
      values[key] = Number.parseFloat(input.value);
    } else {
      values[key] = input.value;
    }
  });
  return values;
}

export function initEvents(): void {
  initMarkdownInteractions();
  document.addEventListener("click", (event) => {
    const target = eventElement(event);
    if (!target) return;
    const personaNameOption = target.closest<HTMLButtonElement>("[data-persona-name-variant]");
    if (personaNameOption) {
      if (selectPersonaNameVariant(personaNameOption.dataset.personaNameVariant || "")) {
        els.personaNameSelector.open = false;
        window.requestAnimationFrame(() => els.personaNameSummary.focus());
      }
      return;
    }
    if (els.personaNameSelector.open && !target.closest("#personaNameSelector")) {
      els.personaNameSelector.open = false;
    }
    const retryButton = target.closest<HTMLElement>("[data-retry-projection]");
    if (retryButton) {
      retryProjection(retryButton.dataset.retryProjection || "");
      return;
    }

    if (target.closest("[data-export-evidence]")) {
      exportCurrentEvidence();
      return;
    }

    if (target.closest("[data-retry-protocol-catalog]")) {
      void pollProtocolCatalog({ force: true });
      return;
    }

    if (target.closest("[data-reload-settings]")) {
      void pollSettings({ force: true });
      return;
    }

    if (target.closest("[data-reload-about]")) {
      void pollAbout({ force: true });
      return;
    }

    const copyDiagnostics = target.closest<HTMLButtonElement>("[data-copy-about-diagnostics]");
    if (copyDiagnostics) {
      const text = aboutDiagnosticText();
      if (!text) return;
      void navigator.clipboard.writeText(text).then(() => {
        copyDiagnostics.textContent = t("已复制");
        window.setTimeout(() => { copyDiagnostics.textContent = t("复制诊断信息"); }, 1500);
      }).catch(() => {
        copyDiagnostics.textContent = t("复制失败");
      });
      return;
    }

    if (target.closest("[data-retry-persona-core]")) {
      void pollPersonaCore({ force: true });
      return;
    }

    if (target.closest("[data-retry-persona-state]")) {
      void pollPersonaState({ force: true, ignoreVisibility: true });
      return;
    }

    if (target.closest("#globalSettingsToggle")) {
      if (state.globalSettingsOpen) closeGlobalSettings();
      else openGlobalSettings();
      return;
    }

    if (target.closest("#configureModelButton")) {
      openGlobalSettings("models");
      return;
    }

    if (target.closest("[data-close-global-settings]")) {
      closeGlobalSettings();
      return;
    }

    const globalTab = target.closest<HTMLElement>("[data-global-settings-tab]");
    if (globalTab) {
      const tab = globalTab.dataset.globalSettingsTab;
      if (tab !== "interface" && tab !== "models" && tab !== "about") return;
      state.globalSettingsTab = tab;
      renderGlobalSettings();
      if (tab === "about") void pollAbout();
      return;
    }

    const newCatalog = target.closest<HTMLElement>("[data-new-catalog]");
    if (newCatalog) {
      const entity = newCatalog.dataset.newCatalog;
      if (entity === "connection") state.editingConnectionId = "new";
      if (entity === "model") state.editingModelId = "new";
      renderGlobalSettings();
      return;
    }

    const editCatalog = target.closest<HTMLElement>("[data-edit-catalog]");
    if (editCatalog) {
      const entity = editCatalog.dataset.editCatalog;
      const id = editCatalog.dataset.catalogId || "";
      if (entity === "connection") state.editingConnectionId = id;
      if (entity === "model") state.editingModelId = id;
      renderGlobalSettings();
      return;
    }

    if (target.closest("[data-cancel-catalog-edit]")) {
      state.editingConnectionId = null;
      state.editingModelId = null;
      renderGlobalSettings();
      return;
    }

    const deleteCatalog = target.closest<HTMLElement>("[data-delete-catalog]");
    if (deleteCatalog) {
      const entity = deleteCatalog.dataset.deleteCatalog;
      const id = deleteCatalog.dataset.catalogId || "";
      if ((entity !== "connection" && entity !== "model") || !id) return;
      if (!window.confirm(t("确认删除这一配置？"))) return;
      void submitModelCatalog(entity, "delete", id, {});
      return;
    }

    const providerKey = target.closest<HTMLElement>("[data-provider-key-action]");
    if (providerKey) {
      const connectionId = providerKey.dataset.providerKeyConnection;
      const action = providerKey.dataset.providerKeyAction;
      if (
        !connectionId
        || (action !== "set" && action !== "delete")
        || !settingsProjection.data
      ) return;
      if (action === "delete") {
        if (!window.confirm(t("确认删除这一密钥？"))) return;
        void submitProviderKey(connectionId, "delete", "");
        return;
      }
      const input = document.querySelector<HTMLInputElement>(`[data-provider-key-input="${CSS.escape(connectionId)}"]`);
      if (!input || !input.value.trim()) {
        input?.focus();
        input?.reportValidity();
        return;
      }
      const key = input.value.trim();
      input.value = "";
      void submitProviderKey(connectionId, "set", key);
      return;
    }

    const retryManual = target.closest<HTMLElement>("[data-retry-manual]");
    if (retryManual) {
      void openManual(retryManual.dataset.retryManual || state.manualFile, { retry: true });
      return;
    }

    const retryMemory = target.closest<HTMLElement>("[data-retry-memory-detail]");
    if (retryMemory) {
      const itemId = retryMemory.dataset.memoryId || "";
      void loadDepositionDetail("memory", itemId, { force: true, render: false })
        .then(() => openMemoryDetail(itemId, { retry: true }));
      return;
    }

    const protocolDocument = target.closest<HTMLElement>("[data-protocol-document], [data-retry-protocol-document]");
    if (protocolDocument) {
      const kind = protocolDocument.dataset.protocolKind;
      const itemId = protocolDocument.dataset.protocolId || "";
      if ((kind === "rule" || kind === "doc") && itemId) {
        void openProtocolDocument(kind, itemId, { retry: protocolDocument.hasAttribute("data-retry-protocol-document") });
      }
      return;
    }

    const ledgerEvent = target.closest<HTMLElement>("[data-ledger-event]");
    if (ledgerEvent) {
      const round = Number(ledgerEvent.dataset.ledgerRound);
      const cardId = ledgerEvent.dataset.ledgerCardId || "";
      if (Number.isInteger(round) && cardId) openLedgerEvent(round, cardId);
      return;
    }

    const contextTool = target.closest<HTMLElement>("[data-context-tool]");
    if (contextTool) {
      openContextToolAnnotation(contextTool.dataset.contextTool || "");
      return;
    }

    if (target.closest("[data-close-system-window]")) {
      closeSystemWindow();
      return;
    }

    const overviewSectionButton = target.closest<HTMLElement>("[data-overview-section]");
    if (overviewSectionButton) {
      const sectionId = overviewSectionButton.dataset.overviewSection;
      if (!sectionId) return;
      if (state.overviewSectionsCollapsed.has(sectionId)) state.overviewSectionsCollapsed.delete(sectionId);
      else state.overviewSectionsCollapsed.add(sectionId);
      renderOverview();
      return;
    }

    if (target.closest("[data-runtime-relay]")) {
      submitRuntimeRelay();
      return;
    }

    if (target.closest("#stopButton")) {
      void submitRuntimeStop();
      return;
    }

    const pageButton = target.closest<HTMLElement>("[data-page]");
    if (pageButton) {
      const pageId = pageButton.dataset.page as PageId;
      const tabId = pageButton.dataset.tab || "";
      const fromSurfaceNav = Boolean(pageButton.closest("#surfaceNav"));
      const togglesSystemWindow = fromSurfaceNav || pageButton.matches(".persona-avatar");
      const isActiveTarget = state.systemWindowOpen
        && state.activePage === pageId
        && (!tabId || getActivePageTab(pageId) === tabId);
      const sourceRound = Number(pageButton.dataset.ledgerSourceRound);
      if (pageId === "audit" && Number.isInteger(sourceRound)) state.selectedLedgerRound = sourceRound;
      const returnFocus: SystemReturnFocus = fromSurfaceNav
        ? { pageId, tabId }
        : pageButton;
      rememberSystemReturnFocus(returnFocus);
      if (fromSurfaceNav) {
        collapseNavNow({ force: true });
      }
      if (togglesSystemWindow && isActiveTarget) {
        closeSystemWindow();
        return;
      }
      setPage(pageId, tabId);
      if (pageId === "persona") void pollPersonaProjection();
      loadActiveDepositionDetail(pageId);
      if (fromSurfaceNav) {
        window.requestAnimationFrame(() => {
          const selector = tabId
            ? `[data-page="${CSS.escape(pageId)}"][data-tab="${CSS.escape(tabId)}"]`
            : `[data-page="${CSS.escape(pageId)}"]:not([data-tab])`;
          els.surfaceNav.querySelector<HTMLElement>(selector)?.focus();
        });
      }
      return;
    }

    const tabButton = target.closest<HTMLElement>("[data-page-tab]");
    if (tabButton) {
      setActivePageTab(state.activePage, tabButton.dataset.pageTab || "");
      renderStageAndFocus(state.activePage, `[data-page-tab="${CSS.escape(state.activeTabs[state.activePage])}"]`);
      if (state.activePage === "persona") void pollPersonaProjection();
      loadActiveDepositionDetail(state.activePage);
      return;
    }

    const runtimePaneButton = target.closest<HTMLElement>("[data-runtime-pane]");
    if (runtimePaneButton) {
      state.activeRuntimePane = runtimePaneButton.dataset.runtimePane || "";
      setActivePageTab("context", "content");
      renderStageAndFocus("context", `[data-runtime-pane="${CSS.escape(state.activeRuntimePane)}"]`);
      return;
    }

    const focusButton = target.closest<HTMLElement>("[data-container-focus-action]");
    if (focusButton) {
      submitContainerFocus(focusButton.dataset.containerFocusAction || "", focusButton.dataset.containerId || "");
      return;
    }

    const depositionButton = target.closest<HTMLElement>("[data-deposition-kind][data-deposition-id]");
    if (depositionButton) {
      const kind = depositionButton.dataset.depositionKind as DepositionKind;
      const itemId = depositionButton.dataset.depositionId || "";
      if (kind === "memory") {
        state.selectedMemoryId = itemId;
        openMemoryDetail(itemId);
        void loadDepositionDetail(kind, itemId, { render: false })
          .then(() => openMemoryDetail(itemId, { retry: true }));
        return;
      } else if (kind === "container") state.selectedContainerId = itemId;
      else state.selectedRelationId = itemId;
      renderStageAndFocus(depositionPage(kind), `[data-deposition-kind="${kind}"][data-deposition-id="${CSS.escape(itemId)}"]`);
      loadDepositionDetail(kind, itemId);
      return;
    }

    const depositionJumpButton = target.closest<HTMLElement>("[data-deposition-jump-kind][data-deposition-id]");
    if (depositionJumpButton) {
      const kind = depositionJumpButton.dataset.depositionJumpKind as DepositionKind;
      const itemId = depositionJumpButton.dataset.depositionId || "";
      if (kind === "memory") {
        state.selectedMemoryId = itemId;
        setPage("mem", "map");
        els.stagePage.querySelector<HTMLElement>(`[data-deposition-kind="memory"][data-deposition-id="${CSS.escape(itemId)}"]`)?.focus();
        openMemoryDetail(itemId);
        void loadDepositionDetail(kind, itemId, { render: false })
          .then(() => openMemoryDetail(itemId, { retry: true }));
        return;
      } else if (kind === "container") {
        state.selectedContainerId = itemId;
        const prefix = itemId.split("-")[0];
        const tab = ({ PRJ: "project", DC: "dc", EC: "ec", SKL: "skl" } as Record<string, string>)[prefix] || "project";
        setPage("containers", tab);
      }
      loadDepositionDetail(kind, itemId);
      return;
    }

    const manualTrigger = target.closest<HTMLElement>("[data-manual]");
    if (manualTrigger) {
      openManual(manualTrigger.dataset.manual || state.manualFile);
      return;
    }

    if (target.closest("[data-close-manual]")) {
      closeManual();
    }
  });

  els.navLockToggle.addEventListener("click", (event) => {
    event.stopPropagation();
    state.navCollapseLocked = !state.navCollapseLocked;
    localStorage.setItem("upsp.v4.navCollapseLocked", state.navCollapseLocked ? "1" : "0");
    collapseNavNow();
    els.app.classList.remove("nav-force-collapsed");
    syncShellState();
    renderNavigation();
  });

  els.leftRail.addEventListener("pointerenter", () => {
    openNav();
  });

  els.leftRail.addEventListener("pointerleave", () => {
    els.app.classList.remove("nav-force-collapsed");
    scheduleNavCollapse();
  });

  els.overviewToggle.addEventListener("click", () => {
    state.overviewCollapsed = !state.overviewCollapsed;
    syncShellState();
  });

  els.chatThread.addEventListener("keydown", (event) => {
    const target = eventElement(event);
    const summary = target?.closest<HTMLElement>(".chat-tool-group > summary, .chat-tool-step > summary");
    if (!summary || !["Enter", " "].includes(event.key)) return;
    event.preventDefault();
    const details = summary.parentElement;
    if (details instanceof HTMLDetailsElement) details.open = !details.open;
  });

  els.runtimeComposer.addEventListener("submit", (event) => void submitRuntimeMessage(event as SubmitEvent));
  document.addEventListener("submit", (event) => {
    const form = eventElement(event)?.closest<HTMLFormElement>("form");
    if (!form) return;
    if (form.matches("[data-interface-settings-form]")) {
      event.preventDefault();
      if (!form.reportValidity()) return;
      const locale = form.querySelector<HTMLSelectElement>("[data-interface-locale]")?.value;
      if (locale === "system" || locale === "zh-CN" || locale === "en-US") {
        void submitSettings([["interface", { locale }]]);
      }
      return;
    }
    if (form.matches("[data-routing-settings-form]")) {
      event.preventDefault();
      if (!form.reportValidity()) return;
      const routes: JsonObject = {};
      for (const phase of ["setup", "reaction", "cleanup"] as const) {
        const slots: Array<JsonObject | null> = [];
        for (const slot of ["primary", "backup_1", "backup_2"] as const) {
          const modelSelect = form.querySelector<HTMLSelectElement>(`[data-route-model][data-route-phase="${phase}"][data-route-slot="${slot}"]`);
          const effortSelect = form.querySelector<HTMLSelectElement>(`[data-route-effort][data-route-phase="${phase}"][data-route-slot="${slot}"]`);
          const modelId = modelSelect?.value || "";
          slots.push(modelId ? { model_id: modelId, reasoning_effort: effortSelect?.value || "" } : null);
        }
        routes[phase] = { primary: slots[0], backups: [slots[1], slots[2]] };
      }
      void submitSettings([["model_routing", {
        cross_phase_failover_enabled: form.querySelector<HTMLInputElement>("[data-cross-phase-failover]")?.checked !== false,
        routes,
      }]]);
      return;
    }
    if (form.matches("[data-model-catalog-form]")) {
      event.preventDefault();
      if (!form.reportValidity()) return;
      const entity = form.dataset.modelCatalogForm;
      if (entity !== "connection" && entity !== "model") return;
      const fields = new FormData(form);
      const text = (name: string) => String(fields.get(name) || "").trim();
      let values: JsonObject;
      if (entity === "connection") {
        values = {
          alias: text("alias"),
          protocol: text("protocol"),
          url: text("url"),
          api_key_env: text("api_key_env"),
        };
      } else {
        let requestOverrides: JsonObject;
        try {
          const parsed: unknown = JSON.parse(text("request_overrides") || "{}");
          if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("not_object");
          requestOverrides = parsed as JsonObject;
        } catch {
          settingsProjection.error = t("兼容请求参数必须是 JSON 对象");
          renderGlobalSettings();
          return;
        }
        values = {
          alias: text("alias"),
          connection_id: text("connection_id"),
          model: text("model"),
          context_window: Number.parseInt(text("context_window"), 10),
          reasoning_supported: text("reasoning_supported").split(",").map((item) => item.trim()).filter(Boolean),
          reasoning_default: text("reasoning_default"),
          streaming_enabled: fields.get("streaming_enabled") === "on",
          streaming_include_usage: fields.get("streaming_include_usage") === "on",
          prompt_cache_profile: text("prompt_cache_profile") || "off",
          request_overrides: requestOverrides,
        };
      }
      const id = form.dataset.modelCatalogId || null;
      void submitModelCatalog(entity, id ? "update" : "create", id, values);
      return;
    }
    if (form.matches("[data-transport-settings-form]")) {
      event.preventDefault();
      if (!form.reportValidity() || !settingsProjection.data) return;
      const values = new FormData(form);
      const integer = (name: string) => Number.parseInt(String(values.get(name) || "0"), 10);
      const current = JSON.parse(JSON.stringify(settingsProjection.data.model_catalog.transport)) as JsonObject;
      const handshake = (current.handshake && typeof current.handshake === "object" ? current.handshake : {}) as JsonObject;
      const breaker = (current.circuit_breaker && typeof current.circuit_breaker === "object" ? current.circuit_breaker : {}) as JsonObject;
      Object.assign(handshake, {
        timeout_seconds: integer("timeout_seconds"),
        retry: integer("retry"),
        request_timeout_seconds: integer("request_timeout_seconds"),
        stream_first_chunk_timeout_seconds: integer("stream_first_chunk_timeout_seconds"),
        stream_idle_timeout_seconds: integer("stream_idle_timeout_seconds"),
        stream_content_overrun_chars: integer("stream_content_overrun_chars"),
      });
      Object.assign(breaker, {
        max_failures: integer("max_failures"),
        cooldown_seconds: integer("cooldown_seconds"),
      });
      current.handshake = handshake;
      current.circuit_breaker = breaker;
      void submitSettings([["models", { transport: current }]]);
      return;
    }
    if (!form.matches("[data-settings-form], [data-context-settings-form]")) return;
    event.preventDefault();
    if (!form.reportValidity()) return;
    const roots = form.hasAttribute("data-context-settings-form")
      ? Array.from(form.querySelectorAll<HTMLElement>("[data-settings-file]"))
      : [form];
    const updates: Array<[SettingsFileId, Record<string, SettingValue>]> = [];
    for (const root of roots) {
      const fileId = (root.dataset.settingsFile || form.dataset.settingsForm) as SettingsFileId;
      if (!["system", "now", "lately", "periodic", "high_freq", "relation"].includes(fileId)) return;
      updates.push([fileId, settingValues(root)]);
    }
    void submitSettings(updates);
  });
  els.permissionLevel.addEventListener("change", () => {
    if (els.permissionLevel.value === "limited") runtimeProjection.unlimitedConfirmed = false;
    runtimeProjection.sendFeedback = els.permissionLevel.value === "unlimited"
      ? t("完整权限将在提交时要求明确确认。")
      : "";
    refreshRuntimeUi();
    if (state.activePage === "run" && getActivePageTab("run") === "tools") renderStage("run");
  });
  document.addEventListener("change", (event) => {
    const selector = eventElement(event)?.closest<HTMLSelectElement>("[data-route-model]");
    if (!selector || !settingsProjection.data) return;
    const phase = selector.dataset.routePhase || "";
    const slot = selector.dataset.routeSlot || "";
    const effort = document.querySelector<HTMLSelectElement>(`[data-route-effort][data-route-phase="${CSS.escape(phase)}"][data-route-slot="${CSS.escape(slot)}"]`);
    if (!effort) return;
    const model = settingsProjection.data.model_catalog.models.find((item) => item.id === selector.value);
    effort.disabled = !model;
    const supported = model?.reasoning.supported?.length ? model.reasoning.supported : [""];
    effort.innerHTML = supported.map((value) => `<option value="${value}">${value || t("系统默认")}</option>`).join("");
    effort.value = model?.reasoning.default || supported[0] || "";
  });
  document.addEventListener("change", (event) => {
    const selector = eventElement(event)?.closest<HTMLSelectElement>("[data-ledger-round]");
    if (!selector) return;
    state.selectedLedgerRound = selector.value === "latest" ? null : Number(selector.value);
    renderStageAndFocus("audit", "[data-ledger-round]");
  });
  document.addEventListener("change", (event) => {
    const selector = eventElement(event)?.closest<HTMLSelectElement>("[data-context-round]");
    if (!selector) return;
    state.selectedContextRound = selector.value === "latest" ? null : Number(selector.value);
    state.selectedContextFrame = null;
    renderStageAndFocus("context", "[data-context-round]");
  });
  document.addEventListener("change", (event) => {
    const selector = eventElement(event)?.closest<HTMLSelectElement>("[data-context-frame]");
    if (!selector) return;
    state.selectedContextFrame = selector.value === "latest" ? null : selector.value;
    renderStageAndFocus("context", "[data-context-frame]");
  });
  els.messageInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && (event.ctrlKey || event.metaKey)) {
      event.preventDefault();
      els.runtimeComposer.requestSubmit();
    }
  });

  document.addEventListener("input", (event) => {
    const search = eventElement(event)?.closest<HTMLInputElement>("[data-memory-search]");
    if (!search) return;
    state.memoryQuery = search.value;
    renderStage("mem");
    window.requestAnimationFrame(() => {
      const next = els.stagePage.querySelector<HTMLInputElement>("[data-memory-search]");
      next?.focus();
      next?.setSelectionRange(next.value.length, next.value.length);
    });
  });

  document.addEventListener("keydown", handleKeyboard);
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) return;
    pollRuntime({ ignoreVisibility: true });
    pollTaskProjection({ ignoreVisibility: true });
    pollDeposition({ ignoreVisibility: true });
  });
  window.addEventListener("resize", () => window.requestAnimationFrame(syncNavPointer));
}

function handleKeyboard(event: KeyboardEvent): void {
  if (event.key === "Escape" && els.personaNameSelector.open) {
    event.preventDefault();
    els.personaNameSelector.open = false;
    els.personaNameSummary.focus();
    return;
  }
  if (state.globalSettingsOpen) {
    if (event.key === "Escape") {
      event.preventDefault();
      closeGlobalSettings();
      return;
    }
    if (event.key === "Tab") {
      const focusable = [...els.globalSettingsOverlay.querySelectorAll<HTMLElement>('button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [href], [tabindex]:not([tabindex="-1"])')]
        .filter((item) => !item.hidden && item.offsetParent !== null);
      if (focusable.length) {
        const first = focusable[0];
        const last = focusable[focusable.length - 1];
        if (event.shiftKey && document.activeElement === first) {
          event.preventDefault();
          last.focus();
        } else if (!event.shiftKey && document.activeElement === last) {
          event.preventDefault();
          first.focus();
        }
      }
    }
    return;
  }
  if (!els.manualOverlay.hidden) {
    if (event.key === "Escape") {
      event.preventDefault();
      closeManual();
      return;
    }
    if (event.key === "Tab") {
      const focusable = [...els.manualOverlay.querySelectorAll<HTMLElement>('button:not([disabled]), [href], [tabindex]:not([tabindex="-1"])')]
        .filter((item) => !item.hidden && item.offsetParent !== null);
      if (!focusable.length) return;
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    }
    return;
  }

  const protocolEntry = eventElement(event)?.closest<HTMLButtonElement>("[data-ledger-event], [data-protocol-document]");
  if (protocolEntry && ["Enter", " ", "Spacebar"].includes(event.key)) {
    event.preventDefault();
    protocolEntry.click();
    return;
  }

  const tab = eventElement(event)?.closest<HTMLButtonElement>(".page-tab");
  if (tab && ["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) {
    const strip = tab.closest<HTMLElement>(".window-tab-strip");
    if (!strip) return;
    const tabs = [...strip.querySelectorAll<HTMLButtonElement>(".page-tab")];
    let index = tabs.indexOf(tab);
    if (event.key === "Home") index = 0;
    else if (event.key === "End") index = tabs.length - 1;
    else index = (index + (event.key === "ArrowRight" ? 1 : -1) + tabs.length) % tabs.length;
    event.preventDefault();
    const nextTab = tabs[index];
    if (!nextTab) return;
    setActivePageTab(state.activePage, nextTab.dataset.pageTab || "");
    renderStageAndFocus(state.activePage, `[data-page-tab="${CSS.escape(state.activeTabs[state.activePage])}"]`);
    if (state.activePage === "persona") void pollPersonaProjection();
  }
}
