import type {
  BootstrapDraft,
  BootstrapPersonaPreset,
  BootstrapStatusPayload,
  JsonObject,
} from "./contracts";
import { t } from "./i18n";
import type { MessageKey } from "./i18n";
import { bootstrapProjection, els, polling } from "./state";
import { openGlobalSettings } from "./view";

type AxisKey = "S" | "C" | "V" | "A" | "R" | "B";

interface ProviderTestReceipt {
  schema_version: "seed_gui_provider_test_receipt.v1";
  status: "passed";
  model_profile_id: string;
  model_alias: string;
  model: string;
  latency_ms: number;
  test_token: string;
  expires_in_seconds: number;
}

interface PersonaInitReceipt {
  schema_version: "seed_gui_persona_init_receipt.v1";
  status: "created";
  model_setup: "tested" | "skipped";
}

const axes: Array<[AxisKey, string, string]> = [
  ["S", "结构", "体验"],
  ["C", "收敛", "发散"],
  ["V", "证据", "幻想"],
  ["A", "分析", "直觉"],
  ["R", "批判", "协作"],
  ["B", "抽象", "具体"],
];

function escapeHtml(value: unknown): string {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function jsonObject(value: unknown): JsonObject {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as JsonObject
    : {};
}

async function requestJson<T>(path: string, options: RequestInit = {}): Promise<T> {
  const response = await fetch(path, { cache: "no-store", ...options });
  let payload: unknown = {};
  try {
    payload = await response.json();
  } catch {
    payload = {};
  }
  if (!response.ok) {
    const body = jsonObject(payload);
    throw new Error(typeof body.error === "string" ? body.error : `HTTP ${response.status}`);
  }
  return payload as T;
}

function axisBudget(values: Record<AxisKey, number>): number {
  return axes.reduce((sum, [key]) => sum + Math.abs(Number(values[key]) - 50), 0);
}

function axisBounds(values: Record<AxisKey, number>, key: AxisKey): [number, number] {
  const spentElsewhere = axisBudget({ ...values, [key]: 50 });
  const allowance = Math.min(50, Math.max(0, 60 - spentElsewhere));
  return [50 - allowance, 50 + allowance];
}

function profileError(draft: BootstrapDraft): MessageKey | "" {
  if (!draft.name_zh.trim() && !draft.name_en.trim()) return "至少填写中文名或英文名之一。";
  if (!/^[A-Z][A-Z0-9]{1,7}$/.test(draft.abbreviation.trim().toUpperCase())) {
    return "稳定缩写须为 2–8 位大写字母或数字，并以字母开头。";
  }
  const roles = draft.roles.map((item) => item.trim()).filter(Boolean);
  if (roles.length < 1 || roles.length > 3) return "请填写 1–3 项社会定位。";
  if (axisBudget(draft.axes) !== 60) return "六轴点数必须恰好为 60。";
  if (draft.self_description.length > 200) return "位格自述不得超过 200 字。";
  if (draft.traits.some((item) => !item.trim())) return "请恰好填写三项特点。";
  return "";
}

function profileStarted(draft: BootstrapDraft): boolean {
  return Boolean(
    draft.name_zh.trim()
    || draft.name_en.trim()
    || draft.abbreviation.trim()
    || draft.roles.some((item) => item.trim())
    || draft.self_description.trim()
    || draft.traits.some((item) => item.trim())
    || draft.instance_notes.trim()
    || axisBudget(draft.axes),
  );
}

function selectedProfile(): BootstrapPersonaPreset | BootstrapDraft | null {
  if (bootstrapProjection.selection === "preset") return bootstrapProjection.data?.preset || null;
  if (bootstrapProjection.selection === "custom") return bootstrapProjection.draft;
  return null;
}

function customPayload(): JsonObject {
  const draft = bootstrapProjection.draft;
  return {
    id: "custom",
    name_zh: draft.name_zh.trim(),
    name_en: draft.name_en.trim(),
    abbreviation: draft.abbreviation.trim().toUpperCase(),
    roles: draft.roles.map((item) => item.trim()).filter(Boolean),
    axes: { ...draft.axes },
    self_description: draft.self_description.trim(),
    traits: draft.traits.map((item) => item.trim()),
    instance_notes: draft.instance_notes.trim(),
  };
}

function setupSummary(): string {
  const setup = bootstrapProjection.data?.setup_primary;
  if (!setup) {
    return `
      <div class="bootstrap-test-state error">
        <strong>${t("尚未配置起手主模型")}</strong>
        <p>${t("模型配置完成后，再由你显式发起一次连接测试。")}</p>
      </div>
    `;
  }
  return `
    <dl class="bootstrap-model-summary">
      <div><dt>${t("当前模型")}</dt><dd>${escapeHtml(setup.model_alias)} · ${escapeHtml(setup.model)}</dd></div>
      <div><dt>${t("连接")}</dt><dd>${escapeHtml(setup.connection_alias)}</dd></div>
      <div><dt>${t("推理强度")}</dt><dd>${escapeHtml(setup.reasoning_effort)}</dd></div>
      <div><dt>${t("上下文窗口")}</dt><dd>${escapeHtml(setup.context_window.toLocaleString())}</dd></div>
    </dl>
  `;
}

function presetProfile(profile: BootstrapPersonaPreset): string {
  return `
    <div class="bootstrap-profile-summary">
      <p>${t("固定示例，创建前不可编辑。")}</p>
      <dl>
        <div><dt>${t("中文名")}</dt><dd>${escapeHtml(profile.name_zh)}</dd></div>
        <div><dt>${t("英文名")}</dt><dd>${escapeHtml(profile.name_en)}</dd></div>
        <div><dt>${t("稳定缩写")}</dt><dd>${escapeHtml(profile.abbreviation)}</dd></div>
        <div><dt>${t("社会定位")}</dt><dd>${profile.roles.map(escapeHtml).join(" / ")}</dd></div>
        <div><dt>${t("核心六轴")}</dt><dd>${axes.map(([key]) => `${key}${profile.axes[key]}`).join(" / ")} · ${escapeHtml(profile.persona_code)}</dd></div>
        <div><dt>${t("三项特点")}</dt><dd>${profile.traits.map(escapeHtml).join(" / ")}</dd></div>
        <div><dt>${t("位格自述")}</dt><dd>${escapeHtml(profile.self_description)}</dd></div>
      </dl>
    </div>
  `;
}

function customProfile(draft: BootstrapDraft): string {
  const budget = axisBudget(draft.axes);
  return `
    <div class="bootstrap-form-grid">
      <label class="bootstrap-field">
        <span>${t("中文名")}</span>
        <input data-bootstrap-field="name_zh" value="${escapeHtml(draft.name_zh)}" autocomplete="off">
      </label>
      <label class="bootstrap-field">
        <span>${t("英文名")}</span>
        <input data-bootstrap-field="name_en" value="${escapeHtml(draft.name_en)}" autocomplete="off">
      </label>
      <label class="bootstrap-field">
        <span>${t("稳定缩写")}</span>
        <input data-bootstrap-field="abbreviation" value="${escapeHtml(draft.abbreviation)}" maxlength="8" pattern="[A-Z][A-Z0-9]{1,7}" autocomplete="off">
      </label>
      <div class="bootstrap-field">
        <span>${t("社会定位")}</span>
        <div class="bootstrap-inline-fields">
          ${draft.roles.map((value, index) => `<input data-bootstrap-field="roles.${index}" value="${escapeHtml(value)}" aria-label="${t("社会定位")} ${index + 1}" placeholder="${t("社会定位")} ${index + 1}" autocomplete="off">`).join("")}
        </div>
      </div>
      <div class="bootstrap-field wide">
        <span>${t("核心六轴")}</span>
        <div class="bootstrap-axis-list">
          ${axes.map(([key, left, right]) => `
            <div class="bootstrap-axis">
              <label for="bootstrapAxis${key}">${t(left as MessageKey)} ↔ ${t(right as MessageKey)}</label>
              <input id="bootstrapAxis${key}" type="range" min="0" max="100" step="1" value="${draft.axes[key]}" data-bootstrap-field="axes.${key}">
              <output data-bootstrap-axis-output="${key}">${draft.axes[key]}</output>
            </div>
          `).join("")}
        </div>
        <p class="bootstrap-budget ${budget === 60 ? "" : "invalid"}" data-bootstrap-budget>${t("自由点数：已用 {used} / 60", { used: budget })}</p>
        <small>${t("六轴必须恰好使用 60 点；向左右任一方向偏移都会消耗点数。")}</small>
      </div>
      <label class="bootstrap-field wide">
        <span>${t("位格自述")} · ${t("不超过 200 字")}</span>
        <textarea data-bootstrap-field="self_description" maxlength="200">${escapeHtml(draft.self_description)}</textarea>
      </label>
      <div class="bootstrap-field wide">
        <span>${t("三项特点")}</span>
        <div class="bootstrap-inline-fields">
          ${draft.traits.map((value, index) => `<input data-bootstrap-field="traits.${index}" value="${escapeHtml(value)}" aria-label="${t("三项特点")} ${index + 1}" placeholder="${t("三项特点")} ${index + 1}" autocomplete="off">`).join("")}
        </div>
      </div>
      <label class="bootstrap-field wide">
        <span>${t("实例补充说明（可选）")}</span>
        <textarea data-bootstrap-field="instance_notes">${escapeHtml(draft.instance_notes)}</textarea>
      </label>
    </div>
  `;
}

function previewProfile(profile: BootstrapPersonaPreset | BootstrapDraft): string {
  const roles = profile.roles.filter(Boolean);
  const traits = profile.traits.filter(Boolean);
  const code = "persona_code" in profile
    ? profile.persona_code
    : axes.map(([key, _left, _right]) => {
      const rightCodes: Record<AxisKey, string> = { S: "E", C: "D", V: "F", A: "I", R: "O", B: "K" };
      return profile.axes[key] === 50 ? "X" : profile.axes[key] > 50 ? key : rightCodes[key];
    }).join("");
  return `
    <dl>
      <div><dt>${t("中文名")}</dt><dd>${escapeHtml(profile.name_zh || "—")}</dd></div>
      <div><dt>${t("英文名")}</dt><dd>${escapeHtml(profile.name_en || "—")}</dd></div>
      <div><dt>${t("稳定缩写")}</dt><dd>${escapeHtml(profile.abbreviation)}</dd></div>
      <div><dt>${t("社会定位")}</dt><dd>${roles.map(escapeHtml).join(" / ")}</dd></div>
      <div><dt>${t("核心六轴")}</dt><dd>${axes.map(([key]) => `${key}${profile.axes[key]}`).join(" / ")} · ${code}</dd></div>
      <div><dt>${t("三项特点")}</dt><dd>${traits.map(escapeHtml).join(" / ")}</dd></div>
      <div><dt>${t("位格自述")}</dt><dd>${escapeHtml(profile.self_description || "—")}</dd></div>
    </dl>
  `;
}

function renderChoice(): string {
  return `
    <div class="bootstrap-shell">
      <div class="bootstrap-brand"><span class="upsp-mark" aria-hidden="true"></span><strong>UPSP</strong></div>
      <header class="bootstrap-hero">
        <span class="hud-label">${t("首次使用 UPSP")}</span>
        <h1>${t("先建立一位可以继续成长的位格主体")}</h1>
      </header>
      <div class="bootstrap-choice-grid">
        <button class="bootstrap-choice recommended" type="button" data-bootstrap-choice="preset">
          <span class="choice-badge">${t("推荐")}</span>
          <strong>${t("使用阿廖沙快速开始")}</strong>
          <p>${t("一个温和、可靠、重视记忆与归返的示例位格。")}</p>
        </button>
        <button class="bootstrap-choice" type="button" data-bootstrap-choice="custom">
          <span class="choice-badge">${t("自定义")}</span>
          <strong>${t("创建自己的位格")}</strong>
          <p>${t("从名称、社会定位与六轴开始。")}</p>
        </button>
      </div>
    </div>
  `;
}

function renderWorkspace(): string {
  const data = bootstrapProjection.data!;
  const profile = selectedProfile();
  const profileProblem = bootstrapProjection.selection === "custom"
    ? profileError(bootstrapProjection.draft)
    : "";
  const customError = profileProblem && profileStarted(bootstrapProjection.draft)
    ? profileProblem
    : "";
  const tested = data.provider_test.valid && Boolean(bootstrapProjection.testToken);
  const modelStepComplete = tested || bootstrapProjection.skipModelSetup;
  const canPreview = Boolean(profile) && !profileProblem && modelStepComplete;
  const testState = bootstrapProjection.pending
    ? `<div class="bootstrap-test-state">${escapeHtml(bootstrapProjection.feedback)}</div>`
    : bootstrapProjection.error
      ? `<div class="bootstrap-test-state error">${escapeHtml(bootstrapProjection.error)}</div>`
      : tested
        ? `<div class="bootstrap-test-state passed">${t("测试通过")}</div>`
        : bootstrapProjection.skipModelSetup
          ? `<div class="bootstrap-test-state">${t("已选择暂不配置模型。创建后可进入 GUI，但发送消息前仍须完成模型配置。")}</div>`
        : bootstrapProjection.testToken
          ? `<div class="bootstrap-test-state error">${t("测试结果已因配置变化或超时失效，请重新测试。")}</div>`
          : `<div class="bootstrap-test-state">${t("模型尚未通过本次初始化测试。")}</div>`;
  const profileBody = bootstrapProjection.selection === "preset" && data.preset
    ? presetProfile(data.preset)
    : customProfile(bootstrapProjection.draft);

  return `
    <div class="bootstrap-shell">
      <div class="bootstrap-brand"><span class="upsp-mark" aria-hidden="true"></span><strong>UPSP</strong></div>
      <header class="bootstrap-hero">
        <button class="bootstrap-action quiet" type="button" data-bootstrap-back>← ${t("返回选择")}</button>
        <h1>${bootstrapProjection.selection === "preset" ? t("使用阿廖沙快速开始") : t("创建自己的位格")}</h1>
      </header>
      <div class="bootstrap-workspace">
        <div class="bootstrap-main">
          <section class="bootstrap-section">
            <header class="bootstrap-section-head"><h2>${t("位格档案")}</h2></header>
            ${profileBody}
          </section>
          <section class="bootstrap-section">
            <header class="bootstrap-section-head"><h2>${t("模型准备")}</h2></header>
            ${setupSummary()}
            <div class="bootstrap-actions">
              <button class="bootstrap-action" type="button" data-bootstrap-models>${t("配置模型服务")}</button>
              <button class="bootstrap-action" type="button" data-bootstrap-test ${data.setup_primary && !bootstrapProjection.pending ? "" : "disabled"}>${t("测试起手主模型（将产生一次付费请求）")}</button>
              <button class="bootstrap-action quiet" type="button" data-bootstrap-skip-model ${bootstrapProjection.pending ? "disabled" : ""}>${t("暂不配置，先创建位格")}</button>
            </div>
            <p class="bootstrap-paid-warning">${t("测试只调用当前起手主模型；点击测试会产生一次真实付费请求，但不会创建轮次、装配位格上下文或尝试备用模型。")}</p>
            ${testState}
          </section>
          <section class="bootstrap-section">
            <header class="bootstrap-section-head"><h2>${t("最终确认")}</h2></header>
            ${bootstrapProjection.preview && profile
              ? `<div class="bootstrap-profile-summary">${previewProfile(profile)}</div>
                 <p>${bootstrapProjection.skipModelSetup
                   ? t("位格将以“模型未绑定”状态创建；进入 GUI 后可随时配置。")
                   : t("确认档案与模型后，宿主会一次性原子创建位格。")}</p>
                 <div class="bootstrap-actions">
                   <button class="bootstrap-action quiet" type="button" data-bootstrap-edit>${t("返回编辑")}</button>
                   <button class="bootstrap-action primary" type="button" data-bootstrap-create ${bootstrapProjection.pending ? "disabled" : ""}>${t("创建位格")}</button>
                 </div>`
              : `<div class="bootstrap-actions">
                   <button class="bootstrap-action primary" type="button" data-bootstrap-preview ${canPreview ? "" : "disabled"}>${t("预览并创建")}</button>
                 </div>`}
            ${customError ? `<div class="bootstrap-test-state error" data-bootstrap-profile-error>${t(customError)}</div>` : ""}
          </section>
        </div>
        <aside class="bootstrap-aside">
          <h2>${t("首次使用 UPSP")}</h2>
          <ol class="bootstrap-step-list">
            <li class="done"><span>1</span><span>${t("位格档案")}</span></li>
            <li class="${modelStepComplete ? "done" : "active"}"><span>2</span><span>${t("模型准备")}</span></li>
            <li class="${bootstrapProjection.preview ? "active" : ""}"><span>3</span><span>${t("最终确认")}</span></li>
          </ol>
        </aside>
      </div>
    </div>
  `;
}

export function bootstrapReady(): boolean {
  return bootstrapProjection.data?.persona.ready === true;
}

export function applyBootstrapGate(): void {
  const ready = bootstrapReady();
  els.bootstrapRoot.hidden = ready;
  els.app.hidden = !ready;
  els.app.toggleAttribute("inert", !ready);
  const abbreviation = bootstrapProjection.data?.identity?.abbreviation || "UPSP";
  document.querySelectorAll<HTMLElement>("[data-persona-abbreviation]").forEach((element) => {
    element.textContent = abbreviation;
  });
}

export function renderBootstrap(): void {
  applyBootstrapGate();
  if (bootstrapReady()) return;
  const data = bootstrapProjection.data;
  if (bootstrapProjection.loading && !data) {
    els.bootstrapRoot.innerHTML = `
      <div class="bootstrap-loading"><span class="upsp-mark" aria-hidden="true"></span><p>${t("正在检查初始化状态")}</p></div>
    `;
    return;
  }
  if (!data) {
    els.bootstrapRoot.innerHTML = `
      <div class="bootstrap-shell"><div class="bootstrap-error">${escapeHtml(bootstrapProjection.error || t("位格创建失败"))}</div></div>
    `;
    return;
  }
  if (data.persona.state === "incomplete") {
    els.bootstrapRoot.innerHTML = `
      <div class="bootstrap-shell">
        <div class="bootstrap-brand"><span class="upsp-mark" aria-hidden="true"></span><strong>UPSP</strong></div>
        <header class="bootstrap-hero"><h1>${t("位格目录已存在但不完整。为保护现场，初始化不会覆盖它。")}</h1></header>
        <div class="bootstrap-error">${escapeHtml(data.persona.missing.join(", ") || data.setup_error)}</div>
      </div>
    `;
    return;
  }
  els.bootstrapRoot.innerHTML = bootstrapProjection.selection === "choice"
    ? renderChoice()
    : renderWorkspace();
}

export function pollBootstrapStatus(): Promise<boolean> {
  if (polling.bootstrap) return polling.bootstrap;
  const request = (async () => {
    try {
      const wasReady = bootstrapProjection.data?.persona.ready;
      const payload = await requestJson<BootstrapStatusPayload>("./api/bootstrap/status");
      if (payload.schema_version !== "seed_gui_bootstrap_status.v1") {
        throw new Error("bootstrap_status_schema_mismatch");
      }
      const nextKey = JSON.stringify(payload);
      const changed = nextKey !== bootstrapProjection.renderKey;
      bootstrapProjection.data = payload;
      bootstrapProjection.renderKey = nextKey;
      bootstrapProjection.error = "";
      if (!payload.provider_test.valid) bootstrapProjection.testToken = "";
      if (wasReady === false && payload.persona.ready) {
        window.location.reload();
        return true;
      }
      if (changed) renderBootstrap();
    } catch (error: unknown) {
      bootstrapProjection.error = error instanceof Error ? error.message : String(error);
      renderBootstrap();
    } finally {
      bootstrapProjection.loading = false;
    }
    return true;
  })();
  polling.bootstrap = request.finally(() => {
    polling.bootstrap = null;
  });
  return polling.bootstrap;
}

async function testProvider(): Promise<void> {
  bootstrapProjection.skipModelSetup = false;
  bootstrapProjection.pending = true;
  bootstrapProjection.error = "";
  bootstrapProjection.feedback = t("正在测试模型连接");
  renderBootstrap();
  try {
    const receipt = await requestJson<ProviderTestReceipt>("./api/bootstrap/provider-test", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });
    if (receipt.schema_version !== "seed_gui_provider_test_receipt.v1" || receipt.status !== "passed") {
      throw new Error("provider_test_receipt_mismatch");
    }
    bootstrapProjection.testToken = receipt.test_token;
    bootstrapProjection.skipModelSetup = false;
    bootstrapProjection.feedback = t("测试通过");
    await pollBootstrapStatus();
  } catch (error: unknown) {
    bootstrapProjection.testToken = "";
    bootstrapProjection.error = `${t("模型测试失败")}：${error instanceof Error ? error.message : String(error)}`;
  } finally {
    bootstrapProjection.pending = false;
    renderBootstrap();
  }
}

async function createPersona(): Promise<void> {
  const token = bootstrapProjection.testToken;
  const skipped = bootstrapProjection.skipModelSetup;
  if (!skipped && (!token || !bootstrapProjection.data?.provider_test.valid)) return;
  bootstrapProjection.pending = true;
  bootstrapProjection.error = "";
  bootstrapProjection.feedback = t("正在创建位格");
  renderBootstrap();
  try {
    const preset = bootstrapProjection.selection === "preset";
    const receipt = await requestJson<PersonaInitReceipt>("./api/bootstrap/persona", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        mode: preset ? "preset" : "custom",
        preset_id: preset ? "alyosha" : null,
        profile: preset ? null : customPayload(),
        test_token: skipped ? null : token,
        skip_model_setup: skipped,
      }),
    });
    if (receipt.schema_version !== "seed_gui_persona_init_receipt.v1" || receipt.status !== "created") {
      throw new Error("persona_init_receipt_mismatch");
    }
    bootstrapProjection.feedback = t("创建成功，正在进入 Seed GUI");
    window.location.reload();
  } catch (error: unknown) {
    bootstrapProjection.error = `${t("位格创建失败")}：${error instanceof Error ? error.message : String(error)}`;
    bootstrapProjection.pending = false;
    renderBootstrap();
  }
}

function updateDraft(input: HTMLInputElement | HTMLTextAreaElement): void {
  const field = input.dataset.bootstrapField || "";
  const draft = bootstrapProjection.draft;
  if (field.startsWith("axes.")) {
    const key = field.slice(5) as AxisKey;
    if (axes.some(([axis]) => axis === key)) {
      const [minimum, maximum] = axisBounds(draft.axes, key);
      draft.axes[key] = Math.max(minimum, Math.min(maximum, Number(input.value)));
      input.value = String(draft.axes[key]);
    }
  } else if (field.startsWith("roles.")) {
    const index = Number(field.slice(6));
    if (index >= 0 && index < 3) draft.roles[index] = input.value;
  } else if (field.startsWith("traits.")) {
    const index = Number(field.slice(7));
    if (index >= 0 && index < 3) draft.traits[index] = input.value;
  } else if (field === "name_zh" || field === "name_en" || field === "self_description" || field === "instance_notes") {
    draft[field] = input.value;
  } else if (field === "abbreviation") {
    draft.abbreviation = input.value.toUpperCase();
    input.value = draft.abbreviation;
  }
  bootstrapProjection.preview = false;
  bootstrapProjection.error = "";
  const budget = axisBudget(draft.axes);
  els.bootstrapRoot.querySelector<HTMLElement>("[data-bootstrap-budget]")?.classList.toggle("invalid", budget !== 60);
  const budgetElement = els.bootstrapRoot.querySelector<HTMLElement>("[data-bootstrap-budget]");
  if (budgetElement) budgetElement.textContent = t("自由点数：已用 {used} / 60", { used: budget });
  if (field.startsWith("axes.")) {
    const key = field.slice(5);
    const output = els.bootstrapRoot.querySelector<HTMLOutputElement>(`[data-bootstrap-axis-output="${key}"]`);
    if (output) output.value = input.value;
  }
  const preview = els.bootstrapRoot.querySelector<HTMLButtonElement>("[data-bootstrap-preview]");
  if (preview) {
    preview.disabled = Boolean(profileError(draft))
      || (!bootstrapProjection.skipModelSetup && (
        !bootstrapProjection.data?.provider_test.valid
        || !bootstrapProjection.testToken
      ));
  }
}

export function initBootstrapEvents(): void {
  document.addEventListener("click", (event) => {
    const target = event.target instanceof Element ? event.target : null;
    if (!target || !target.closest("#bootstrapRoot")) return;
    const choice = target.closest<HTMLElement>("[data-bootstrap-choice]");
    if (choice) {
      bootstrapProjection.selection = choice.dataset.bootstrapChoice === "custom" ? "custom" : "preset";
      bootstrapProjection.preview = false;
      bootstrapProjection.skipModelSetup = false;
      bootstrapProjection.error = "";
      renderBootstrap();
      return;
    }
    if (target.closest("[data-bootstrap-back]")) {
      bootstrapProjection.selection = "choice";
      bootstrapProjection.preview = false;
      bootstrapProjection.skipModelSetup = false;
      bootstrapProjection.error = "";
      renderBootstrap();
      return;
    }
    if (target.closest("[data-bootstrap-models]")) {
      openGlobalSettings("models");
      return;
    }
    if (target.closest("[data-bootstrap-test]")) {
      void testProvider();
      return;
    }
    if (target.closest("[data-bootstrap-skip-model]")) {
      bootstrapProjection.skipModelSetup = true;
      bootstrapProjection.testToken = "";
      bootstrapProjection.preview = true;
      bootstrapProjection.error = "";
      renderBootstrap();
      return;
    }
    if (target.closest("[data-bootstrap-preview]")) {
      const error = bootstrapProjection.selection === "custom"
        ? profileError(bootstrapProjection.draft)
        : "";
      if (error) {
        bootstrapProjection.error = t(error);
      } else {
        bootstrapProjection.preview = true;
      }
      renderBootstrap();
      return;
    }
    if (target.closest("[data-bootstrap-edit]")) {
      bootstrapProjection.preview = false;
      renderBootstrap();
      return;
    }
    if (target.closest("[data-bootstrap-create]")) void createPersona();
  });
  els.bootstrapRoot.addEventListener("input", (event) => {
    const target = event.target;
    if (target instanceof HTMLInputElement || target instanceof HTMLTextAreaElement) {
      if (target.dataset.bootstrapField) updateDraft(target);
    }
  });
  document.addEventListener("upsp:locale-changed", renderBootstrap);
}
