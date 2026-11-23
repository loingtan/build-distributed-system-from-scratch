import { useEffect, useRef } from 'react';
import { basicSetup } from 'codemirror';
import { EditorView, keymap } from '@codemirror/view';
import { EditorState, Prec } from '@codemirror/state';
import { indentWithTab } from '@codemirror/commands';
import { javascript } from '@codemirror/lang-javascript';
import { go } from '@codemirror/lang-go';
import { indentUnit } from '@codemirror/language';
import { lintGutter, setDiagnostics } from '@codemirror/lint';
import { HighlightStyle, syntaxHighlighting } from '@codemirror/language';
import { tags as t } from '@lezer/highlight';

// Colours come from CSS variables, so the editor follows the page theme.
const theme = EditorView.theme({
  '&': { height: '100%', fontSize: '13.5px', backgroundColor: 'var(--code-bg)', color: 'var(--ink)' },
  '.cm-scroller': { fontFamily: 'var(--mono)', lineHeight: '1.65' },
  '.cm-content': { padding: '14px 0', caretColor: 'var(--accent)' },
  '.cm-gutters': { backgroundColor: 'var(--code-bg)', color: 'var(--faint)', border: 'none', paddingLeft: '6px' },
  '.cm-activeLine': { backgroundColor: 'var(--code-line)' },
  '.cm-activeLineGutter': { backgroundColor: 'var(--code-line)', color: 'var(--muted)' },
  '&.cm-focused': { outline: 'none' },
  '.cm-cursor': { borderLeftColor: 'var(--accent)', borderLeftWidth: '2px' },
  '&.cm-focused .cm-selectionBackground, .cm-selectionBackground, ::selection': { backgroundColor: 'var(--code-select)' },
  '.cm-matchingBracket': { backgroundColor: 'var(--accent-soft)', outline: 'none' },
  '.cm-tooltip': { backgroundColor: 'var(--surface)', border: '1px solid var(--line)', borderRadius: '8px', color: 'var(--ink)' },
  '.cm-tooltip-autocomplete ul li[aria-selected]': { backgroundColor: 'var(--accent)', color: 'var(--on-accent)' },
  '.cm-panels': { backgroundColor: 'var(--surface)', color: 'var(--ink)' },
});

const highlight = HighlightStyle.define([
  { tag: [t.keyword, t.controlKeyword, t.operatorKeyword, t.definitionKeyword, t.moduleKeyword], color: 'var(--syn-keyword)' },
  { tag: [t.string, t.special(t.string), t.regexp], color: 'var(--syn-string)' },
  { tag: [t.number, t.bool, t.null, t.atom], color: 'var(--syn-number)' },
  { tag: [t.lineComment, t.blockComment], color: 'var(--syn-comment)', fontStyle: 'italic' },
  { tag: [t.function(t.variableName), t.function(t.propertyName)], color: 'var(--syn-function)' },
  { tag: t.propertyName, color: 'var(--syn-property)' },
  { tag: [t.definition(t.variableName), t.typeName, t.className], color: 'var(--ink)' },
]);

export default function CodeEditor({ docKey, initial, onChange, onRun, readOnly = false, language = 'js', diagnostics = null }) {
  const host = useRef(null);
  const viewRef = useRef(null);
  const handlers = useRef({ onChange, onRun });
  handlers.current = { onChange, onRun };

  // The editor owns the text. It is rebuilt only when docKey changes
  // (another challenge, or a reset).
  useEffect(() => {
    const view = new EditorView({
      parent: host.current,
      state: EditorState.create({
        doc: initial,
        extensions: [
          Prec.highest(
            keymap.of([
              {
                key: 'Mod-Enter',
                run: () => {
                  handlers.current.onRun?.();
                  return true;
                },
              },
            ]),
          ),
          basicSetup,
          keymap.of([indentWithTab]),
          ...(language === 'go' ? [go(), indentUnit.of('\t')] : [javascript()]),
          theme,
          syntaxHighlighting(highlight),
          EditorState.readOnly.of(readOnly),
          lintGutter(),
          EditorView.updateListener.of((u) => {
            if (u.docChanged) handlers.current.onChange?.(u.state.doc.toString());
          }),
        ],
      }),
    });
    viewRef.current = view;
    return () => {
      viewRef.current = null;
      view.destroy();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [docKey, readOnly, language]);

  // Compiler errors: [{ line, col, message }] (1-based), marked in the gutter and underlined.
  useEffect(() => {
    const view = viewRef.current;
    if (!view) return;
    const doc = view.state.doc;
    const diags = (diagnostics ?? [])
      .filter((d) => d.line >= 1 && d.line <= doc.lines)
      .map((d) => {
        const line = doc.line(d.line);
        const from = Math.min(line.from + Math.max(0, (d.col ?? 1) - 1), line.to);
        return { from, to: Math.max(from, line.to), severity: 'error', message: d.message };
      });
    view.dispatch(setDiagnostics(view.state, diags));
    if (diags.length) view.dispatch({ selection: { anchor: diags[0].from }, scrollIntoView: true });
  }, [diagnostics, docKey]);

  return <div className="editor-host" ref={host} />;
}
