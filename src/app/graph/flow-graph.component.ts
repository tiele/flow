import {
  AfterViewInit, Component, ElementRef, EventEmitter, Input,
  OnChanges, OnDestroy, Output, SimpleChanges, ViewChild
} from '@angular/core';
import { NgIf } from '@angular/common';

// Use ESM-style imports compatible with Angular's TS config
import cytoscape from 'cytoscape';
import dagre from 'cytoscape-dagre';

import { Cluster, RuntimeDag } from '../models';

// Register plugin (cast avoids typing mismatch)
(cytoscape as any).use(dagre);

@Component({
  selector: 'app-flow-graph',
  standalone: true,
  imports: [NgIf],
  templateUrl: './flow-graph.component.html',
  styleUrls: ['./flow-graph.component.scss']
})
export class FlowGraphComponent implements AfterViewInit, OnChanges, OnDestroy {
  @Input() dag?: RuntimeDag;
  @Input() editMode = false;
  @Output() selectNode = new EventEmitter<string>();
  @Output() addLink = new EventEmitter<{ source: string; target: string }>();
  @Output() removeLink = new EventEmitter<string>();
  @Output() nodePositionsChange = new EventEmitter<Array<{ key: string; x: number; y: number }>>();
  @ViewChild('cyHost', { static: true }) cyHost!: ElementRef<HTMLDivElement>;

  private cy?: cytoscape.Core;
  private resizeObs?: ResizeObserver;
  private pendingSourceId?: string;
  private pendingSourceNode?: cytoscape.NodeSingular;

  ngAfterViewInit() {
    if (this.dag) this.build();
    // Resize observer to keep Cytoscape canvas in sync with container
    this.resizeObs = new ResizeObserver(() => {
      this.cy?.resize();
    });
    this.resizeObs.observe(this.cyHost.nativeElement);
    this.updateEditModeState();
  }
  ngOnChanges(ch: SimpleChanges) {
    if (ch['dag']) {
      this.clearPendingSource();
      if (this.cy) this.build();
    }
    if (ch['editMode']) this.updateEditModeState();
  }
  ngOnDestroy() { this.resizeObs?.disconnect(); this.cy?.destroy(); }

  private build() {
    const elements: cytoscape.ElementDefinition[] = [];
    const presetPositions = new Map<string, { x: number; y: number }>();

    (this.dag?.clusters ?? []).forEach((c: Cluster) => {
      elements.push({ data: { id: c.id, label: c.label ?? c.id, isCluster: true } });
    });

    this.dag?.nodes.forEach(n => {
      const parent = (this.dag?.clusters ?? []).find(c => c.childNodeIds.includes(n.key))?.id;
      // Global is determined strictly by node key prefix
      const isGlobal = n.key.startsWith('GLOBAL-');
      if (n.display && isFinite(n.display.x) && isFinite(n.display.y)) {
        presetPositions.set(n.key, { x: n.display.x, y: n.display.y });
      }
      elements.push({
        data: {
          id: n.key,
          label: n.label,
          ntype: n.rule?.type ?? n.type ?? 'STEP',
          category: n.rule?.category ?? '',
          parent,
          isGlobal: isGlobal ? 1 : undefined
        }
      });
    });

    this.dag?.links.forEach(l => {
      elements.push({ data: { id: l.id, source: l.source, target: l.target, label: l.label ?? '' } });
    });

    this.cy?.destroy();
    this.cy = cytoscape({
      container: this.cyHost.nativeElement,
      elements,
      style: this.styles() as any
    } as any);

    const lockedForPreset: cytoscape.NodeSingular[] = [];
    this.cy.nodes().forEach(node => {
      const preset = presetPositions.get(node.id());
      if (preset) {
        node.position({ x: preset.x, y: preset.y });
        node.lock();
        lockedForPreset.push(node);
      }
    });

    // Run layout explicitly so we can reliably hook layoutstop
    const layout = this.cy.layout({ name: 'dagre', rankDir: 'LR', nodeSep: 50, rankSep: 80, edgeSep: 10 } as any);
    layout.one('layoutstop', () => {
      lockedForPreset.forEach(n => n.unlock());
      this.positionIsolatedNodes(new Set(presetPositions.keys()));
      this.cy?.fit(undefined, 24);
      this.emitAllPositions();
    });
    layout.run();

    this.cy!.on('tap', 'node', (evt: cytoscape.EventObject) => this.onNodeTap(evt));
    this.cy!.on('tap', 'edge', (evt: cytoscape.EventObject) => this.onEdgeTap(evt));
    this.cy!.on('tap', (evt: cytoscape.EventObject) => {
      if (evt.target === this.cy) this.clearPendingSource();
    });
    this.cy!.on('dragfree', 'node', (evt: cytoscape.EventObject) => {
      const node = evt.target as cytoscape.NodeSingular;
      if (node.data('isCluster')) return;
      this.emitPositions(this.collectionFor(node));
    });
  }

  private onNodeTap(evt: cytoscape.EventObject) {
    const node = evt.target as cytoscape.NodeSingular;
    if (node.data('isCluster')) return;
    if (this.editMode) {
      this.handleEditNodeTap(node);
      return;
    }
    this.clearPendingSource();
    this.selectNode.emit(node.id());
  }

  private onEdgeTap(evt: cytoscape.EventObject) {
    if (!this.editMode) return;
    evt.preventDefault();
    evt.stopPropagation();
    const edge = evt.target as cytoscape.EdgeSingular;
    this.removeLink.emit(edge.id());
  }

  private handleEditNodeTap(node: cytoscape.NodeSingular) {
    const nodeId = node.id();
    if (!this.pendingSourceId) {
      this.setPendingSource(node);
      return;
    }
    if (this.pendingSourceId === nodeId) {
      this.clearPendingSource();
      return;
    }
    const source = this.pendingSourceId;
    const target = nodeId;
    if (source === target) {
      this.clearPendingSource();
      return;
    }
    if (this.dag?.links.some(l => l.source === source && l.target === target)) {
      this.clearPendingSource();
      return;
    }
    this.addLink.emit({ source, target });
    this.clearPendingSource();
  }

  private setPendingSource(node?: cytoscape.NodeSingular) {
    if (this.pendingSourceNode) this.pendingSourceNode.removeClass('pending-link-source');
    this.pendingSourceNode = node;
    this.pendingSourceId = node?.id();
    if (this.pendingSourceNode) this.pendingSourceNode.addClass('pending-link-source');
  }

  private clearPendingSource() {
    this.setPendingSource(undefined);
  }

  private updateEditModeState() {
    if (!this.editMode) this.clearPendingSource();
  }

  private positionIsolatedNodes(presetKeys: Set<string>) {
    if (!this.cy) return;
    const allNodes = this.cy.nodes();
    const iso = allNodes.filter(n => !n.data('isCluster') && n.connectedEdges().length === 0 && !presetKeys.has(n.id()));
    if (iso.length === 0) return;

    const anchorNodes = allNodes.filter(n => n.connectedEdges().length > 0 || presetKeys.has(n.id()));
    const referenceBox = anchorNodes.length > 0 ? anchorNodes.boundingBox() : allNodes.boundingBox();
    const baseX = (Number.isFinite(referenceBox.x2) ? referenceBox.x2 : 0) + 200;
    const startY = Number.isFinite(referenceBox.y1) ? referenceBox.y1 : 0;
    const spacingY = 160;

    const sorted = iso.sort((a, b) => a.id().localeCompare(b.id()));
    sorted.forEach((n, idx) => {
      const x = baseX;
      const y = startY + idx * spacingY;
      n.position({ x, y });
    });

    iso.parents().forEach(p => { p.emit('position'); });
  }

  private emitAllPositions() {
    if (!this.cy) return;
    this.emitPositions(this.cy.nodes() as unknown as cytoscape.CollectionReturnValue);
  }

  private emitPositions(nodes: cytoscape.CollectionReturnValue) {
    const updates = nodes
      .filter(ele => ele.isNode() && !ele.data('isCluster'))
      .map(ele => {
        const node = ele as cytoscape.NodeSingular;
        const pos = node.position();
        const x = Number(pos.x.toFixed(2));
        const y = Number(pos.y.toFixed(2));
        if (!isFinite(x) || !isFinite(y)) return undefined;
        const maxAbs = 1e6;
        if (Math.abs(x) > maxAbs || Math.abs(y) > maxAbs) return undefined;
        return { key: node.id(), x, y };
      });
    const filtered = updates.filter((u): u is { key: string; x: number; y: number } => !!u);
    if (filtered.length) this.nodePositionsChange.emit(filtered);
  }

  private collectionFor(node: cytoscape.NodeSingular): cytoscape.CollectionReturnValue {
    if (!this.cy) return (node as unknown) as cytoscape.CollectionReturnValue;
    const anyNode = node as any;
    if (typeof anyNode.collection === 'function') {
      return anyNode.collection();
    }
    return this.cy.nodes().filter(ele => ele.id() === node.id()) as unknown as cytoscape.CollectionReturnValue;
  }

  private styles(): any[] {
    return [
      { selector: '$node > node', style: {
          'shape': 'round-rectangle',
          'background-opacity': 0.03,
          'border-width': 2,
          'border-style': 'dashed',
          'border-color': '#8e9bb3',
          'label': '',
          'text-opacity': 0,
          'padding': '20px'
      }},
      { selector: 'node', style: {
          'shape': 'round-rectangle',
          'background-color': '#f7f9fc',
          'border-width': 2,
          'border-color': '#cfd7e3',
          'label': 'data(label)',
          'font-weight': 600,
          'text-wrap': 'wrap',
          'text-max-width': 140,
          'padding': '8px'
      }},
      { selector: 'node[ntype = "INPUT"]',  style: { 'border-color': '#31c48d', 'background-color': '#ecfdf5' } },
      { selector: 'node[ntype = "FILTER"]', style: { 'border-color': '#8b5cf6', 'background-color': '#f5f3ff' } },
      { selector: 'node[ntype = "ENRICH"]', style: { 'border-color': '#3b82f6', 'background-color': '#eff6ff' } },
      { selector: 'node[ntype = "INVOICE"]',style: { 'border-color': '#f59e0b', 'background-color': '#fffbeb' } },
      { selector: 'edge', style: {
          'curve-style': 'bezier',
          'control-point-step-size': 40,
          'target-arrow-shape': 'triangle',
          'target-arrow-color': '#9aa6b2',
          'line-color': '#9aa6b2',
          'width': 2
      }},
      { selector: 'node:selected', style: {
          'border-color': '#0ea5e9',
          'border-width': 3,
          'background-color': '#e0f2fe'
      }},
      { selector: 'node.pending-link-source', style: {
          'border-color': '#f97316',
          'border-width': 4,
          'background-color': '#fff7ed'
      }},
      // Global badge (default gray), positioned bottom-right
      { selector: 'node[isGlobal = 1]', style: {
          'background-image': 'data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 18 18"><circle cx="9" cy="9" r="9" fill="%23cfd7e3"/><text x="9" y="12" font-size="12" text-anchor="middle" fill="white" font-family="Arial,Helvetica,sans-serif" font-weight="700">G</text></svg>',
          'background-width': '18px',
          'background-height': '18px',
          'background-clip': 'node',
          'background-fit': 'none',
          'background-position-x': '98%',
          'background-position-y': '98%',
          'background-repeat': 'no-repeat'
      }},
      { selector: 'node[isGlobal = 1][ntype = "INPUT"]',  style: {
          'background-image': 'data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 18 18"><circle cx="9" cy="9" r="9" fill="%2331c48d"/><text x="9" y="12" font-size="12" text-anchor="middle" fill="white" font-family="Arial,Helvetica,sans-serif" font-weight="700">G</text></svg>'
      }},
      { selector: 'node[isGlobal = 1][ntype = "FILTER"]', style: {
          'background-image': 'data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 18 18"><circle cx="9" cy="9" r="9" fill="%238b5cf6"/><text x="9" y="12" font-size="12" text-anchor="middle" fill="white" font-family="Arial,Helvetica,sans-serif" font-weight="700">G</text></svg>'
      }},
      { selector: 'node[isGlobal = 1][ntype = "ENRICH"]', style: {
          'background-image': 'data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 18 18"><circle cx="9" cy="9" r="9" fill="%233b82f6"/><text x="9" y="12" font-size="12" text-anchor="middle" fill="white" font-family="Arial,Helvetica,sans-serif" font-weight="700">G</text></svg>'
      }},
      { selector: 'node[isGlobal = 1][ntype = "INVOICE"]',style: {
          'background-image': 'data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 18 18"><circle cx="9" cy="9" r="9" fill="%23f59e0b"/><text x="9" y="12" font-size="12" text-anchor="middle" fill="white" font-family="Arial,Helvetica,sans-serif" font-weight="700">G</text></svg>'
      }}
    ];
  }
}
