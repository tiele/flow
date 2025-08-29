import {
  AfterViewInit, Component, ElementRef, EventEmitter, Input,
  OnChanges, OnDestroy, Output, SimpleChanges, ViewChild
} from '@angular/core';

// Use ESM-style imports compatible with Angular's TS config
import cytoscape from 'cytoscape';
import dagre from 'cytoscape-dagre';

import { Cluster, RuntimeDag } from '../models';

// Register plugin (cast avoids typing mismatch)
(cytoscape as any).use(dagre);

@Component({
  selector: 'app-flow-graph',
  standalone: true,
  templateUrl: './flow-graph.component.html',
  styleUrls: ['./flow-graph.component.scss']
})
export class FlowGraphComponent implements AfterViewInit, OnChanges, OnDestroy {
  @Input() dag?: RuntimeDag;
  @Output() selectNode = new EventEmitter<string>();
  @ViewChild('cyHost', { static: true }) cyHost!: ElementRef<HTMLDivElement>;

  private cy?: cytoscape.Core;
  private resizeObs?: ResizeObserver;

  ngAfterViewInit() {
    if (this.dag) this.build();
    // Resize observer to keep Cytoscape canvas in sync with container
    this.resizeObs = new ResizeObserver(() => {
      this.cy?.resize();
    });
    this.resizeObs.observe(this.cyHost.nativeElement);
  }
  ngOnChanges(ch: SimpleChanges) { if (ch['dag'] && this.cy) this.build(); }
  ngOnDestroy() { this.resizeObs?.disconnect(); this.cy?.destroy(); }

  private build() {
    const elements: cytoscape.ElementDefinition[] = [];

    (this.dag?.clusters ?? []).forEach((c: Cluster) => {
      elements.push({ data: { id: c.id, label: c.label ?? c.id, isCluster: true } });
    });

    this.dag?.nodes.forEach(n => {
      const parent = (this.dag?.clusters ?? []).find(c => c.childNodeIds.includes(n.key))?.id;
      // Global is determined strictly by node key prefix
      const isGlobal = n.key.startsWith('GLOBAL-');
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

    // Run layout explicitly so we can reliably hook layoutstop
    const layout = this.cy.layout({ name: 'dagre', rankDir: 'LR', nodeSep: 50, rankSep: 80, edgeSep: 10 } as any);
    layout.one('layoutstop', () => this.positionIsolatedNodes());
    layout.run();

    this.cy!.on('tap', 'node', (evt: cytoscape.EventObject) => {
      const id = (evt.target as cytoscape.NodeSingular).id();
      if (evt.target.data('isCluster')) return;
      this.selectNode.emit(id);
    });
  }

  private positionIsolatedNodes() {
    if (!this.cy) return;
    const allNodes = this.cy.nodes();
    const iso = allNodes.filter(n => !n.data('isCluster') && n.connectedEdges().length === 0);
    if (iso.length === 0) return;

    // Place relative to current viewport (container), not model bounding box
    const zoom = this.cy.zoom();
    const pan = this.cy.pan(); // { x, y } in rendered px offset of model origin
    const containerW = this.cy.width();
    const containerH = this.cy.height();

    const padRightPx = 24; // visual padding from right edge
    const padTopPx = 24;   // visual padding from top edge
    const spacingPx = 180; // spacing in rendered pixels between isolated nodes

    const sorted = iso.sort((a, b) => a.id().localeCompare(b.id()));
    const totalWidthPx = (sorted.length - 1) * spacingPx;
    const startRenderedX = Math.max(16, containerW - padRightPx - totalWidthPx);
    const renderedY = Math.min(containerH - 80, padTopPx + 0); // ensure inside viewport

    // Convert rendered (px) -> model coords
    const toModel = (rx: number, ry: number) => ({ x: (rx - pan.x) / zoom, y: (ry - pan.y) / zoom });
    const baseModel = toModel(startRenderedX, renderedY);

    sorted.forEach((n, i) => {
      const m = toModel(startRenderedX + i * spacingPx, renderedY);
      n.position({ x: m.x, y: m.y });
    });

    // Nudge parents (clusters) to recompute bounding boxes
    iso.parents().forEach(p => { p.emit('position'); }); // trigger re-render
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
