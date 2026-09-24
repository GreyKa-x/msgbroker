function buildHeadersJson(form) {
  const hidden = form.querySelector("#execute_headers");
  const section = hidden ? hidden.closest("[data-optional-section]") : null;
  const toggle = section ? section.querySelector("[data-optional-toggle]") : null;
  const rows = form.querySelectorAll("[data-header-rows] .header-row");
  if (!hidden) {
    return;
  }
  if (toggle && !toggle.checked) {
    hidden.value = "{}";
    return;
  }

  const headers = {};
  rows.forEach((row) => {
    const keyInput = row.querySelector("[data-header-key]");
    const valueInput = row.querySelector("[data-header-value]");
    const key = keyInput ? keyInput.value.trim() : "";
    const value = valueInput ? valueInput.value : "";
    if (key) {
      headers[key] = value;
    }
  });
  hidden.value = JSON.stringify(headers, null, 2);
}

const toast = document.querySelector("[data-toast]");
if (toast) {
  const url = new URL(window.location.href);
  url.searchParams.delete("toast");
  url.searchParams.delete("toast_type");
  window.history.replaceState({}, "", url);
  window.setTimeout(() => {
    toast.classList.add("is-hiding");
    window.setTimeout(() => toast.remove(), 180);
  }, 3800);
}

const runDialog = document.querySelector("[data-run-dialog]");
const runForm = document.querySelector("[data-run-form]");
const runConfirm = runDialog?.querySelector("[data-confirm-run]");
const runConfirmDefaultLabel = runConfirm ? runConfirm.textContent : "Run Once";

function resetRunConfirm() {
  if (!runConfirm) {
    return;
  }
  runConfirm.disabled = false;
  runConfirm.textContent = runConfirmDefaultLabel;
}

function showClientToast(message, toastType) {
  document.querySelector("[data-toast]")?.remove();
  const toast = document.createElement("div");
  toast.className = `toast toast-${toastType || "error"}`;
  toast.setAttribute("data-toast", "");
  toast.setAttribute("role", "status");
  toast.innerHTML = `<span class="toast-icon" aria-hidden="true"></span><span class="toast-message"></span>`;
  toast.querySelector(".toast-message").textContent = message;
  document.body.prepend(toast);
  window.setTimeout(() => {
    toast.classList.add("is-hiding");
    window.setTimeout(() => toast.remove(), 180);
  }, 4800);
}

document.querySelector("[data-run-once]")?.addEventListener("click", () => {
  resetRunConfirm();
  runDialog?.showModal();
});
runDialog?.querySelector("[data-cancel-run]")?.addEventListener("click", () => {
  resetRunConfirm();
  runDialog.close();
});
runConfirm?.addEventListener("click", () => {
  runConfirm.disabled = true;
  runConfirm.textContent = "Running…";
  runDialog?.close();
  runForm?.requestSubmit();
});
runDialog?.addEventListener("click", (event) => {
  if (event.target === runDialog) {
    resetRunConfirm();
    runDialog.close();
  }
});

runForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  const action = runForm.getAttribute("action");
  if (!action) {
    resetRunConfirm();
    return;
  }
  try {
    const response = await fetch(action, {
      method: "POST",
      redirect: "follow",
      credentials: "same-origin",
    });
    if (response.redirected || response.ok) {
      window.location.assign(response.url || action.replace(/\/run$/, "/executions"));
      return;
    }
    const detail = (await response.text()).trim();
    if (response.status === 409) {
      showClientToast(
        detail || "This job is already running; the duplicate start was rejected.",
        "error",
      );
    } else {
      showClientToast(
        detail || `Run failed (HTTP ${response.status}). Check executions to confirm status.`,
        "error",
      );
    }
    resetRunConfirm();
  } catch (_error) {
    showClientToast(
      "Network timeout or disconnect. Result is unconfirmed — refresh executions to verify. Do not resubmit automatically.",
      "error",
    );
    resetRunConfirm();
  }
});

document.querySelectorAll("[data-health-refresh-form]").forEach((form) => {
  form.addEventListener("submit", () => {
    const button = form.querySelector(".refresh-button");
    if (button) {
      button.disabled = true;
      button.classList.add("is-refreshing");
    }
  });
});

document.querySelectorAll("[data-toggle-form]").forEach((form) => {
  form.addEventListener("submit", (event) => {
    const button = form.querySelector(".toggle-switch");
    if (!button) {
      return;
    }

    event.preventDefault();
    const isOn = button.classList.contains("is-on");
    const action = isOn ? "Disable" : "Enable";
    const jobName = button.dataset.jobName || "this job";
    if (!window.confirm(`${action} job “${jobName}”?`)) {
      return;
    }

    button.classList.toggle("is-on", !isOn);
    button.classList.toggle("is-off", isOn);
    button.setAttribute("aria-checked", String(!isOn));
    button.disabled = true;
    window.setTimeout(() => form.submit(), 240);
  });
});

const deleteDialog = document.querySelector("[data-delete-dialog]");
const deleteForm = deleteDialog?.querySelector("[data-delete-form]");
const deleteName = deleteDialog?.querySelector("[data-delete-job-name]");
const deleteInput = deleteDialog?.querySelector("[data-delete-confirmation]");
const deleteConfirm = deleteDialog?.querySelector("[data-confirm-delete]");

function updateDeleteConfirmation() {
  if (deleteConfirm && deleteInput) {
    deleteConfirm.disabled = deleteInput.value !== deleteInput.dataset.expectedName;
  }
}

document.querySelectorAll("[data-delete-job]").forEach((button) => {
  button.addEventListener("click", () => {
    if (!deleteDialog || !deleteForm || !deleteInput) {
      return;
    }
    deleteForm.action = button.dataset.deleteAction;
    deleteInput.value = "";
    deleteInput.dataset.expectedName = button.dataset.jobName || "";
    if (deleteName) {
      deleteName.textContent = button.dataset.jobName || "";
    }
    updateDeleteConfirmation();
    deleteDialog.showModal();
    deleteInput.focus();
  });
});

deleteInput?.addEventListener("input", updateDeleteConfirmation);
deleteDialog?.querySelector("[data-cancel-delete]")?.addEventListener("click", () => deleteDialog.close());
deleteDialog?.addEventListener("click", (event) => {
  if (event.target === deleteDialog) {
    deleteDialog.close();
  }
});
deleteForm?.addEventListener("submit", (event) => {
  if (!deleteInput || deleteInput.value !== deleteInput.dataset.expectedName) {
    event.preventDefault();
    updateDeleteConfirmation();
  }
});

function createHeaderRow() {
  const row = document.createElement("div");
  row.className = "header-row";
  row.innerHTML = `
    <input data-header-key placeholder="Authorization">
    <input data-header-value placeholder="Bearer token">
    <button type="button" class="token-button" data-insert-token="{{UUID}}" data-header-token>UUID</button>
    <button type="button" data-remove-header title="Remove header">Remove</button>
  `;
  return row;
}

function getJsonTarget(button) {
  return document.getElementById(button.dataset.target);
}

function getJsonValue(target) {
  if (target._jsonEditor) {
    return target._jsonEditor.getValue();
  }
  return target.value;
}

function setJsonValue(target, value) {
  if (target._jsonEditor) {
    target._jsonEditor.setValue(value);
    target._jsonEditor.clearSelection();
  } else {
    target.value = value;
  }
}

function syncJsonTarget(target) {
  if (target._jsonEditor) {
    target.value = target._jsonEditor.getValue();
  }
}

function getJsonStatus(target) {
  const editor = target ? target.closest("[data-json-editor]") : null;
  return editor ? editor.querySelector("[data-json-status]") : null;
}

function setJsonStatus(target, message, state) {
  const status = getJsonStatus(target);
  if (!status) {
    return;
  }
  status.textContent = message;
  status.dataset.state = state;
}

function parseJsonObjectFromTextarea(target) {
  syncJsonTarget(target);
  const raw = getJsonValue(target).trim() || "{}";
  const parsed = JSON.parse(raw);
  if (!parsed || Array.isArray(parsed) || typeof parsed !== "object") {
    throw new Error("Request body must be a JSON object.");
  }
  return parsed;
}

function validateJsonTextarea(target) {
  try {
    parseJsonObjectFromTextarea(target);
    setJsonStatus(target, "Valid JSON", "ok");
    return true;
  } catch (error) {
    setJsonStatus(target, error.message, "error");
    return false;
  }
}

function formatJsonTextarea(target) {
  try {
    setJsonValue(target, JSON.stringify(parseJsonObjectFromTextarea(target), null, 2));
    syncJsonTarget(target);
    setJsonStatus(target, "Formatted", "ok");
    return true;
  } catch (error) {
    setJsonStatus(target, error.message, "error");
    return false;
  }
}

document.addEventListener("click", (event) => {
  const addButton = event.target.closest("[data-add-header]");
  if (addButton) {
    const editor = addButton.closest("[data-headers-editor]");
    const rows = editor ? editor.querySelector("[data-header-rows]") : null;
    if (rows) {
      rows.appendChild(createHeaderRow());
    }
    return;
  }

  const removeButton = event.target.closest("[data-remove-header]");
  if (removeButton) {
    const row = removeButton.closest(".header-row");
    if (row) {
      row.remove();
    }
    return;
  }

  const tokenButton = event.target.matches("[data-insert-token]")
    ? event.target
    : event.target.closest("button[data-insert-token]");
  if (tokenButton) {
    let token = tokenButton.dataset.insertToken || "";
    if (tokenButton.dataset.tokenJsonString === "true") {
      token = JSON.stringify(token);
    }
    if (tokenButton.matches("[data-header-token]")) {
      const row = tokenButton.closest(".header-row");
      const input = row ? row.querySelector("[data-header-value]") : null;
      if (input) {
        const start = input.selectionStart ?? input.value.length;
        const end = input.selectionEnd ?? input.value.length;
        input.value = input.value.slice(0, start) + token + input.value.slice(end);
        input.focus();
        input.selectionStart = start + token.length;
        input.selectionEnd = start + token.length;
      }
      return;
    }

    const target = document.getElementById(tokenButton.dataset.target);
    if (!target) {
      return;
    }
    if (target._jsonEditor) {
      target._jsonEditor.insert(token);
      target._jsonEditor.focus();
      syncJsonTarget(target);
      validateJsonTextarea(target);
      return;
    }
    const start = target.selectionStart ?? target.value.length;
    const end = target.selectionEnd ?? target.value.length;
    target.value = target.value.slice(0, start) + token + target.value.slice(end);
    target.focus();
    target.selectionStart = start + token.length;
    target.selectionEnd = start + token.length;
    validateJsonTextarea(target);
    return;
  }

  const formatButton = event.target.closest("[data-format-json]");
  if (formatButton) {
    const target = getJsonTarget(formatButton);
    if (!target) {
      return;
    }
    formatJsonTextarea(target);
  }
});

document.addEventListener("change", (event) => {
  if (!event.target.matches("[data-optional-toggle]")) {
    return;
  }
  const section = event.target.closest("[data-optional-section]");
  const content = section ? section.querySelector("[data-optional-content]") : null;
  if (!content) {
    return;
  }
  content.hidden = !event.target.checked;
  content.querySelectorAll("[data-json-textarea]").forEach((textarea) => {
    if (textarea._jsonEditor) {
      textarea._jsonEditor.resize();
      textarea._jsonEditor.focus();
    }
  });
});

document.addEventListener("input", (event) => {
  if (event.target.matches("[data-json-textarea]")) {
    validateJsonTextarea(event.target);
  }
});

document.addEventListener("blur", (event) => {
  if (event.target.matches("[data-json-textarea]")) {
    validateJsonTextarea(event.target);
  }
}, true);

document.addEventListener("submit", (event) => {
  buildHeadersJson(event.target);
  const jsonTextareas = event.target.querySelectorAll("[data-json-textarea]");
  for (const textarea of jsonTextareas) {
    const section = textarea.closest("[data-optional-section]");
    const toggle = section ? section.querySelector("[data-optional-toggle]") : null;
    if (toggle && !toggle.checked) {
      textarea.value = "{}";
      continue;
    }
    if (!formatJsonTextarea(textarea)) {
      event.preventDefault();
      textarea.focus();
      break;
    }
  }
});

document.querySelectorAll("[data-json-textarea]").forEach((textarea) => {
  const editorHost = textarea.closest("[data-json-editor]")?.querySelector("[data-ace-json-editor]");
  if (window.ace && editorHost) {
    textarea.hidden = true;
    const editor = window.ace.edit(editorHost);
    editor.setTheme("ace/theme/chrome");
    editor.session.setMode("ace/mode/json");
    editor.session.setUseWorker(false);
    editor.session.setTabSize(2);
    editor.session.setUseSoftTabs(true);
    editor.setOptions({
      fontSize: "13px",
      minLines: 10,
      maxLines: 24,
      showPrintMargin: false,
      wrap: true,
    });
    editor.setValue(textarea.value || "{}", -1);
    textarea._jsonEditor = editor;
    editor.session.on("change", () => {
      syncJsonTarget(textarea);
      validateJsonTextarea(textarea);
    });
    editor.on("blur", () => {
      validateJsonTextarea(textarea);
    });
  }
  formatJsonTextarea(textarea);
});
