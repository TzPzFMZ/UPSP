export interface ConversationStickyState {
  conversationStickToBottom: boolean;
}

export function updateConversationStickyState(
  container: HTMLElement,
  state: ConversationStickyState,
): void {
  const distanceFromBottom = container.scrollHeight - container.scrollTop - container.clientHeight;
  state.conversationStickToBottom = distanceFromBottom <= 24;
}

export function scrollConversationToBottomIfSticky(
  container: HTMLElement,
  state: ConversationStickyState,
): void {
  if (state.conversationStickToBottom) container.scrollTop = container.scrollHeight;
}

export function mutateScrollLayout(
  container: HTMLElement | null,
  element: HTMLElement,
  mutate: () => void,
  shouldStickToBottom?: () => boolean,
): void {
  if (!container) {
    mutate();
    return;
  }
  const distanceFromBottom = container.scrollHeight - container.scrollTop - container.clientHeight;
  const atBottom = distanceFromBottom <= 24;
  const containerTop = container.getBoundingClientRect().top;
  const before = element.getBoundingClientRect();
  const aboveViewport = before.bottom <= containerTop + 1;
  const oldHeight = element.offsetHeight;
  mutate();
  window.requestAnimationFrame(() => {
    const stickToBottom = shouldStickToBottom ? shouldStickToBottom() : atBottom;
    if (stickToBottom) container.scrollTop = container.scrollHeight;
    else if (aboveViewport) container.scrollTop += element.offsetHeight - oldHeight;
  });
}
