const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

function setup(ace) {
  const listeners = {};
  const editorEvents = {};
  const status = { dataset: {} };
  const area = {
    value: '{"value":"abc"}', selectionStart: 0, selectionEnd: 0,
    matches: (selector) => selector === '[data-json-textarea]',
    closest: () => ({ querySelector: (selector) => selector === '[data-json-status]' ? status : {} }),
    focus() {},
  };
  const editor = {
    value: area.value, start: 0, end: 0,
    session: { setMode() {}, setUseWorker() {}, setTabSize() {}, setUseSoftTabs() {}, on() {} },
    setTheme() {}, setOptions() {}, focus() {},
    setValue(value) { this.value = value; this.start = this.end = value.length; },
    clearSelection() { this.end = this.start; },
    getValue() { return this.value; },
    insert(token) {
      this.value = this.value.slice(0, this.start) + token + this.value.slice(this.end);
      this.start += token.length; this.end = this.start;
    },
    on(name, callback) { editorEvents[name] = callback; },
  };
  const document = {
    querySelector() { return null; },
    querySelectorAll(selector) { return selector === '[data-json-textarea]' ? [area] : []; },
    addEventListener(name, callback) { listeners[name] = callback; },
    getElementById() { return area; },
  };
  vm.runInNewContext(fs.readFileSync('internal/web/static/app.js', 'utf8'), {
    document, window: ace ? { ace: { edit: () => editor } } : {},
  });
  const button = {
    dataset: { insertToken: '{{UUID}}', target: 'execute_body' },
    matches: (selector) => selector === '[data-insert-token]',
    closest() { return null; },
  };
  return { area, editor, blur: () => ace ? editorEvents.blur() : listeners.blur({ target: area }),
    click: () => listeners.click({ target: button }) };
}

for (const ace of [true, false]) {
  for (const replace of [false, true]) {
    test(`${ace ? 'Ace' : 'textarea'} inserts UUID at ${replace ? 'selection' : 'cursor'} after blur`, () => {
      const { area, editor, blur, click } = setup(ace);
      const initial = ace ? editor.value : area.value;
      const start = initial.indexOf('abc') + 1;
      const end = start + (replace ? 1 : 0);
      if (ace) { editor.start = start; editor.end = end; }
      else { area.selectionStart = start; area.selectionEnd = end; }
      blur();
      click();
      assert.equal(area.value, initial.slice(0, start) + '{{UUID}}' + initial.slice(end));
      click();
      assert.equal(area.value, initial.slice(0, start) + '{{UUID}}{{UUID}}' + initial.slice(end));
    });
  }
}
