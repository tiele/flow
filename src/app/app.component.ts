import { Component } from '@angular/core';
import { DagLoaderService } from './dag-loader.service';
import { FlowGraphComponent } from './graph/flow-graph.component';
import { NodeInspectorComponent } from './inspector/node-inspector.component';
import { DiffViewerComponent } from './inspector/diff-viewer.component';
import { DagFile, DagLink, RuntimeDag } from './models';
import { NgIf, JsonPipe } from '@angular/common';
import { FormsModule } from '@angular/forms';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [NgIf, JsonPipe, FormsModule, FlowGraphComponent, NodeInspectorComponent, DiffViewerComponent],
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent {
  dag?: RuntimeDag;
  selectedKey?: string;
  sourceType = 'all';
  flowFilter = 'all';
  graphHeight = Math.round(window.innerHeight * 0.46);
  inPayload?: any;
  outPayload?: any;
  focusPaths?: string[];
  viewMode: 'graph' | 'json' = 'graph';
  dagJsonText = '';
  dagJsonError?: string;
  private dagJsonDirty = false;
  editConnections = false;

  constructor(private loader: DagLoaderService) {}

  async loadSample() {
    this.dag = await this.loader.loadFromUrl('assets/output_poseidon.json');
    this.selectedKey = undefined;
    this.clearSelectionState();
    this.refreshDagJson(true);
  }

  async onPickFile(ev: Event) {
    const input = ev.target as HTMLInputElement;
    const file = input.files?.[0];
    if (!file) return;
    this.dag = await this.loader.loadFromFile(file);
    this.selectedKey = undefined;
    this.clearSelectionState();
    this.refreshDagJson(true);
  }

  onSelectNode(key: string) {
    this.selectedKey = key;
    if (!this.dag) return;
    const node = this.dag.nodes.find(n => n.key === key);
    const { inRef, outRef } = this.loader.refsForNode(this.dag, key);
    const io = this.loader.ioForNode(this.dag, key);
    this.inPayload = io.inPayload;
    this.outPayload = io.outPayload;
    // Compute focused paths for JSON diff based on rule category/type
    const cat = (node?.rule?.category || node?.rule?.type || '').toUpperCase();
    this.focusPaths = this.computeFocusPaths(cat);
    const badges: string[] = [];
    if (key.startsWith('GLOBAL-')) badges.push('global');
    // Diagnostic log: selected node, rule reference, and IO references
    console.log('[FlowViewer] node selected:', key, {
      label: node?.label,
      ruleReference: node?.rule?.reference ?? null,
      inRef: inRef ?? null,
      outRef: outRef ?? null,
      badges
    });
  }

  private computeFocusPaths(category: string): string[] | undefined {
    switch (category) {
      case 'DICTIONARY':
        return ['usage.dataDictionary'];
      case 'ALLOCATION':
        return ['costAllocationDetails'];
      case 'PRICING':
        return ['usage.product', 'usage.cost'];
      default:
        return undefined;
    }
  }

  onUpdateRule(e: { key: string; newContent: any; type?: string; category?: string; description?: string; reference?: string }) {
    if (!this.dag) return;
    const n = this.dag.nodes.find(n => n.key === e.key);
    if (!n) return;
    if (!n.rule) n.rule = {
      id: 0,
      type: e.type || n.type || '',
      category: e.category || '',
      description: e.description || '',
      content: e.newContent,
      reference: e.reference || ''
    };
    else {
      n.rule.content = e.newContent;
      if (e.type !== undefined) n.rule.type = e.type;
      if (e.category !== undefined) n.rule.category = e.category;
      if (e.description !== undefined) n.rule.description = e.description;
      if (e.reference !== undefined) n.rule.reference = e.reference;
    }
    // refresh IO preview if same node is selected
    if (this.selectedKey === e.key && this.dag) {
      const io = this.loader.ioForNode(this.dag, e.key);
      this.inPayload = io.inPayload;
      this.outPayload = io.outPayload;
    }
    this.refreshDagJson();
  }

  private dragging = false;
  private startY = 0;
  private startHeight = 0;
  onDragStart(ev: MouseEvent) {
    ev.preventDefault();
    this.dragging = true;
    this.startY = ev.clientY;
    this.startHeight = this.graphHeight;
    const move = (e: MouseEvent) => this.onDragMove(e);
    const up = () => this.onDragEnd(move, up);
    document.addEventListener('mousemove', move);
    document.addEventListener('mouseup', up, { once: true });
    document.body.classList.add('resizing-row');
  }
  private onDragMove(ev: MouseEvent) {
    if (!this.dragging) return;
    const dy = ev.clientY - this.startY;
    const min = 180; // px
    const max = Math.max(min, window.innerHeight - 220);
    this.graphHeight = Math.max(min, Math.min(max, this.startHeight + dy));
  }
  private onDragEnd(move: any, up: any) {
    this.dragging = false;
    document.removeEventListener('mousemove', move);
    document.body.classList.remove('resizing-row');
  }

  setViewMode(mode: 'graph' | 'json') {
    if (mode === this.viewMode) return;
    if (this.viewMode === 'json' && mode === 'graph' && this.dagJsonDirty) {
      const applied = this.applyDagJsonInternal();
      if (!applied) {
        this.viewMode = 'json';
        return;
      }
    }
    this.viewMode = mode;
    if (mode === 'json') {
      this.editConnections = false;
      this.refreshDagJson(true);
      this.dagJsonError = undefined;
    }
  }

  onDagJsonChange(value: string) {
    this.dagJsonText = value;
    this.dagJsonDirty = true;
    this.dagJsonError = undefined;
  }

  applyDagJson() {
    this.applyDagJsonInternal();
  }

  toggleEditConnections() {
    if (this.viewMode !== 'graph') return;
    this.editConnections = !this.editConnections;
  }

  onAddLink(e: { source: string; target: string }) {
    if (!this.dag) return;
    const { source, target } = e;
    if (this.dag.links.some(l => l.source === source && l.target === target)) return;
    const id = this.generateLinkId();
    const newLink: DagLink = { id, source, target, dataReference: null };
    const links = [...this.dag.links, newLink];
    const nodes = this.dag.nodes.map(n => {
      if (n.key === source) {
        const outputs = this.addToList(n.outputs, id);
        return { ...n, outputs };
      }
      if (n.key === target) {
        const inputs = this.addToList(n.inputs, id);
        return { ...n, inputs };
      }
      return n;
    });
    this.dag = { ...this.dag, links, nodes };
    this.refreshDagJson();
    this.reconcileSelection();
  }

  onRemoveLink(linkId: string) {
    if (!this.dag) return;
    const link = this.dag.links.find(l => l.id === linkId);
    if (!link) return;
    const links = this.dag.links.filter(l => l.id !== linkId);
    const nodes = this.dag.nodes.map(n => {
      if (n.key === link.source) {
        const outputs = (n.outputs ?? []).filter(id => id !== linkId);
        return { ...n, outputs };
      }
      if (n.key === link.target) {
        const inputs = (n.inputs ?? []).filter(id => id !== linkId);
        return { ...n, inputs };
      }
      return n;
    });
    this.dag = { ...this.dag, links, nodes };
    this.refreshDagJson();
    this.reconcileSelection();
  }

  private applyDagJsonInternal(): boolean {
    const text = this.dagJsonText.trim();
    if (!text) {
      this.dag = undefined;
      this.selectedKey = undefined;
      this.clearSelectionState();
      this.dagJsonError = undefined;
      this.dagJsonDirty = false;
      this.editConnections = false;
      return true;
    }
    try {
      const raw = JSON.parse(text) as DagFile;
      const normalized = this.loader.normalize(raw);
      this.dag = normalized;
      this.dagJsonError = undefined;
      this.dagJsonDirty = false;
      this.refreshDagJson(true);
      this.reconcileSelection();
      return true;
    } catch (err: any) {
      const message = err?.message ?? 'Invalid DAG JSON';
      this.dagJsonError = `Invalid DAG JSON: ${message}`;
      return false;
    }
  }

  private refreshDagJson(force = false) {
    if (!this.dag) {
      this.dagJsonText = '';
      this.dagJsonDirty = false;
      return;
    }
    if (!force && this.viewMode === 'json' && this.dagJsonDirty) {
      return;
    }
    const raw = this.loader.toRaw(this.dag);
    this.dagJsonText = JSON.stringify(raw, null, 2);
    this.dagJsonDirty = false;
  }

  private reconcileSelection() {
    if (!this.dag) {
      this.selectedKey = undefined;
      this.clearSelectionState();
      return;
    }
    if (this.selectedKey && this.dag.nodes.some(n => n.key === this.selectedKey)) {
      this.onSelectNode(this.selectedKey);
    } else {
      this.selectedKey = undefined;
      this.clearSelectionState();
    }
  }

  private clearSelectionState() {
    this.inPayload = undefined;
    this.outPayload = undefined;
    this.focusPaths = undefined;
  }

  private generateLinkId(): string {
    const used = new Set(this.dag?.links.map(l => l.id) ?? []);
    let counter = (this.dag?.links.length ?? 0) + 1;
    let candidate = `link-${counter}`;
    while (used.has(candidate)) {
      counter++;
      candidate = `link-${counter}`;
    }
    return candidate;
  }

  private addToList(list: string[] | undefined, value: string): string[] {
    const next = [...(list ?? [])];
    if (!next.includes(value)) next.push(value);
    return next;
  }
}
