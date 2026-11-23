// Everything the learner does is kept in this browser's localStorage:
// their code, which challenges they passed, hints revealed, layout.
import { useSyncExternalStore } from 'react';

const KEY = 'quorum-lab:v1';
// `passed` holds tasks passed in JavaScript, `passedGo` those passed in Go.
const defaults = { code: {}, passed: {}, passedGo: {}, hints: {}, revealed: {}, theme: null, lang: 'js', docker: false, briefWidth: 40, editorHeight: 46 };

function load() {
  try {
    return { ...defaults, ...JSON.parse(localStorage.getItem(KEY) ?? '{}') };
  } catch {
    return { ...defaults };
  }
}

let state = load();
const listeners = new Set();

// What pages read: `passed` is the list for the language selected in the
// editor, so progress bars and ticks follow the language you practise in.
const viewOf = (s) => ({ ...s, passed: s.lang === 'go' ? s.passedGo : s.passed, passedJs: s.passed });
let view = viewOf(state);

export function update(patch) {
  state = { ...state, ...(typeof patch === 'function' ? patch(state) : patch) };
  view = viewOf(state);
  try {
    localStorage.setItem(KEY, JSON.stringify(state));
  } catch {
    // Storage may be unavailable (private window); the session still works.
  }
  listeners.forEach((fn) => fn());
}

export const setIn = (field, key, value) => update((s) => ({ [field]: { ...s[field], [key]: value } }));

export function useStore() {
  return useSyncExternalStore(
    (fn) => {
      listeners.add(fn);
      return () => listeners.delete(fn);
    },
    () => view,
  );
}

export const getState = () => state;
