
import {
  applyBootstrapGate,
  bootstrapReady,
  initBootstrapEvents,
  pollBootstrapStatus,
  renderBootstrap,
} from "./bootstrap";
import { initEvents } from "./events";
import {
  pollDeposition,
  pollAbout,
  pollPersonaState,
  pollProtocolCatalog,
  pollRuntime,
  runtimePollingActive,
  pollSettings,
  pollTaskProjection,
} from "./runtime";
import { els } from "./state";
import { render } from "./view";

function startRuntimeUi(): void {
  void pollRuntime();
  void pollDeposition();
  void pollTaskProjection();
  void pollProtocolCatalog();
  window.setInterval(() => {
    if (!runtimePollingActive()) void pollRuntime();
  }, 1500);
  window.setInterval(() => {
    if (runtimePollingActive()) void pollRuntime();
  }, 500);
  window.setInterval(pollDeposition, 6000);
  window.setInterval(pollPersonaState, 6000);
}

async function start(): Promise<void> {
  initEvents();
  initBootstrapEvents();
  els.permissionLevel.value = "limited";
  render();
  renderBootstrap();
  await Promise.all([
    pollAbout({ force: true }),
    pollSettings({ force: true }),
    pollBootstrapStatus(),
  ]);
  applyBootstrapGate();
  if (bootstrapReady()) {
    startRuntimeUi();
  } else {
    window.setInterval(() => void pollBootstrapStatus(), 1500);
  }
}

void start();
