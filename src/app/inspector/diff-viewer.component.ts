import { Component, Input, OnChanges, SimpleChanges } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import * as jdp from 'jsondiffpatch';

@Component({
  selector: 'app-diff-viewer',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './diff-viewer.component.html',
  styleUrls: ['./diff-viewer.component.scss']
})
export class DiffViewerComponent implements OnChanges {
  @Input() left?: any;
  @Input() right?: any;
  @Input() focusPaths?: string[]; // optional list of paths to scope the diff

  html = '';
  summary = { added: 0, changed: 0, removed: 0 };
  // Flattened entries for a side-by-side table
  entries: Array<{ path: string; type: 'added' | 'changed' | 'removed'; left: any; right: any }>= [];
  view: 'table' | 'html' = 'html';
  // Filters
  showAdded = true;
  showChanged = true;
  showRemoved = true;
  // Row expansion state for complex JSON preview
  expanded: Record<string, boolean> = {};
  // Whether to apply focusPaths scoping when computing the diff (default off)
  scopeEnabled = false;

  ngOnChanges(_: SimpleChanges) {
    const lRaw = this.left ?? {};
    const rRaw = this.right ?? {};
    const doScope = this.scopeEnabled && !!this.focusPaths?.length;
    const l = doScope ? this.pickPaths(lRaw, this.focusPaths!) : lRaw;
    const r = doScope ? this.pickPaths(rRaw, this.focusPaths!) : rRaw;
    const delta = jdp.create().diff(l, r);
    this.html = delta ? (jdp as any).formatters.html.format(delta, l) : '';
    const counts = { added: 0, changed: 0, removed: 0 };
    const entries: typeof this.entries = [];
    this.walk(delta, counts, entries);
    // Sort entries by path for stable JSON diffs
    entries.sort((a, b) => a.path.localeCompare(b.path));
    this.summary = counts;
    this.entries = entries;
    this.expanded = {};
  }

  private walk(delta: any, counts: any, entries: typeof this.entries, basePath: string = '') {
    if (!delta || typeof delta !== 'object') return;
    const isArrayDelta = delta && delta._t === 'a';
    Object.keys(delta).forEach(k => {
      if (k === '_t') return;
      const v = (delta as any)[k];
      const keyForPath = isArrayDelta ? `[${k.replace(/^_/, '')}]` : k;
      if (Array.isArray(v)) {
        const path = basePath ? (isArrayDelta ? `${basePath}${keyForPath}` : `${basePath}.${keyForPath}`) : keyForPath;
        // Added
        if (v.length === 1) { counts.added++; entries.push({ path, type: 'added', left: undefined, right: v[0] }); return; }
        // Changed
        if (v.length === 2) { counts.changed++; entries.push({ path, type: 'changed', left: v[0], right: v[1] }); return; }
        // Removed (0 flag)
        if (v.length === 3 && v[2] === 0) { counts.removed++; entries.push({ path, type: 'removed', left: v[0], right: undefined }); return; }
        // Moves and other array ops are ignored in counts/UI for simplicity
      } else if (v && typeof v === 'object') {
        // Handle jsondiffpatch array additions: { _t:'a', '3': { '0': newValue } }
        if (isArrayDelta && Object.prototype.hasOwnProperty.call(v, '0') && !Object.prototype.hasOwnProperty.call(v, '1')) {
          const path = basePath ? `${basePath}${keyForPath}` : keyForPath;
          counts.added++;
          entries.push({ path, type: 'added', left: undefined, right: (v as any)['0'] });
          return;
        }
        const nextPath = basePath ? (isArrayDelta ? `${basePath}${keyForPath}` : `${basePath}.${keyForPath}`) : keyForPath;
        this.walk(v, counts, entries, nextPath);
      }
    });
  }

  private pickPaths(obj: any, paths: string[]): any {
    const out: any = {};
    for (const p of paths) {
      const parts = p.split('.');
      let src: any = obj;
      let dst: any = out;
      for (let i = 0; i < parts.length; i++) {
        const key = parts[i];
        if (src == null || !(key in src)) { src = undefined; break; }
        if (i === parts.length - 1) {
          dst[key] = src[key];
        } else {
          dst[key] = dst[key] ?? {};
          dst = dst[key];
          src = src[key];
        }
      }
    }
    return out;
  }

  // JSON-optimized helpers
  isComplex(v: any): boolean {
    return v !== null && typeof v === 'object';
  }
  preview(v: any): string {
    const t = typeof v;
    if (v === undefined) return '—';
    if (v === null) return 'null';
    if (t === 'string') return JSON.stringify(v);
    if (t === 'number' || t === 'boolean') return String(v);
    if (Array.isArray(v)) return `[… ${v.length} items]`;
    return `{… ${Object.keys(v).length} keys}`;
  }
  pretty(v: any): string {
    const normalize = (x: any): any => {
      if (x === undefined) return undefined;
      if (x === null || typeof x !== 'object') return x;
      if (Array.isArray(x)) return x.map(normalize);
      // sort object keys for stable output
      return Object.keys(x).sort().reduce((acc: any, k) => { acc[k] = normalize(x[k]); return acc; }, {});
    };
    try {
      const norm = normalize(v);
      if (norm === undefined) return 'undefined';
      return JSON.stringify(norm, null, 2);
    } catch {
      return String(v);
    }
  }
  toggleExpand(path: string) {
    this.expanded[path] = !this.expanded[path];
  }
  filteredEntries() {
    return this.entries.filter(e =>
      (e.type === 'added' && this.showAdded) ||
      (e.type === 'changed' && this.showChanged) ||
      (e.type === 'removed' && this.showRemoved)
    );
  }
}
