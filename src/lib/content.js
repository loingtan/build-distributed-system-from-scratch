// Loads the text that goes with each challenge: brief, starter, solution.
const briefs = import.meta.glob('../curriculum/briefs/*.md', { query: '?raw', import: 'default', eager: true });
const code = import.meta.glob('../curriculum/code/*.js', { query: '?raw', import: 'default', eager: true });
import api from '../curriculum/api.md?raw';
import apiGo from '../curriculum/api-go.md?raw';

export const brief = (slug) => briefs[`../curriculum/briefs/${slug}.md`] ?? '';
export const starter = (slug) => code[`../curriculum/code/${slug}.starter.js`] ?? '';
export const solution = (slug) => code[`../curriculum/code/${slug}.solution.js`] ?? '';
export const apiReference = api;
export const apiReferenceGo = apiGo;
