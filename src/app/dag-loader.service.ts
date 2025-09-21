import { Injectable } from '@angular/core';
import { Cluster, DagFile, DagLink, DagNode, NodeDisplay, RuntimeDag } from './models';

@Injectable({ providedIn: 'root' })
export class DagLoaderService {
  async loadFromFile(file: File): Promise<RuntimeDag> {
    const text = await file.text();
    const raw: DagFile = JSON.parse(text);
    return this.normalize(raw);
  }
  async loadFromUrl(url: string): Promise<RuntimeDag> {
    const raw: DagFile = await fetch(url).then(r => r.json());
    return this.normalize(raw);
  }
  normalize(raw: DagFile): RuntimeDag {
    const nodes: DagNode[] = Object.entries(raw.nodes || {}).map(([key, n]) => ({
      key,
      label: n.label ?? key,
      type: n.type ?? null,
      inputs: n.inputs ?? [],
      outputs: n.outputs ?? [],
      display: this.parseDisplay(n.display),
      rule: n.rule ? {
        id: n.rule.id,
        type: n.rule.type,
        category: n.rule.category,
        description: n.rule.description,
        content: n.rule.content,
        reference: n.rule.reference
      } : null
    }));
    const links: DagLink[] = Object.entries(raw.links || {}).map(([key, l]) => ({
      id: key,
      label: l.label,
      source: l.source,
      target: l.target,
      dataReference: l.dataReference ?? null
    }));
    const clusters: Cluster[] = raw.clusters ?? [];
    const data = raw.data ?? {};
    return { nodes, links, clusters, data };
  }
  // Return only the dataReference keys used for a node's IN/OUT
  refsForNode(dag: RuntimeDag, nodeKey: string): { inRef?: string; outRef?: string } {
    const node = dag.nodes.find(n => n.key === nodeKey);
    // IN: prefer declared first input link
    let inRef: string | undefined;
    if (node?.inputs && node.inputs.length > 0) {
      const inLinkId = node.inputs[0];
      const inLink = dag.links.find(l => l.id === inLinkId);
      inRef = inLink?.dataReference ?? undefined;
    }
    if (!inRef) {
      const incoming = dag.links.find(l => l.target === nodeKey && l.dataReference);
      inRef = incoming?.dataReference ?? undefined;
    }

    // OUT: prefer declared first output link; then `${node}:o`; then any outgoing with dataReference
    let outRef: string | undefined;
    if (node?.outputs && node.outputs.length > 0) {
      const outLinkId = node.outputs[0];
      const outLink = dag.links.find(l => l.id === outLinkId);
      outRef = outLink?.dataReference ?? undefined;
    }
    if (!outRef) {
      const nodeOutKey = `${nodeKey}:o`;
      if (Object.prototype.hasOwnProperty.call(dag.data, nodeOutKey)) {
        outRef = nodeOutKey;
      }
    }
    if (!outRef) {
      const outgoing = dag.links.find(l => l.source === nodeKey && l.dataReference);
      outRef = outgoing?.dataReference ?? undefined;
    }
    return { inRef, outRef };
  }
  ioForNode(dag: RuntimeDag, nodeKey: string): { inPayload?: any; outPayload?: any } {
    const { inRef, outRef } = this.refsForNode(dag, nodeKey);
    return {
      inPayload: inRef ? dag.data[inRef] : undefined,
      outPayload: outRef ? dag.data[outRef] : undefined
    };
  }

  toRaw(dag: RuntimeDag): DagFile {
    const nodes: Record<string, any> = {};
    dag.nodes.forEach(n => {
      const entry: Record<string, any> = {};
      entry.label = n.label;
      if (n.type !== undefined) entry.type = n.type;
      if (n.inputs && n.inputs.length) entry.inputs = [...n.inputs];
      if (n.outputs && n.outputs.length) entry.outputs = [...n.outputs];
      if (n.display && isFinite(n.display.x) && isFinite(n.display.y)) {
        entry.display = { x: n.display.x, y: n.display.y };
      }
      if (n.rule) {
        entry.rule = {
          id: n.rule.id,
          type: n.rule.type,
          category: n.rule.category,
          description: n.rule.description,
          content: n.rule.content,
          reference: n.rule.reference
        };
      }
      nodes[n.key] = entry;
    });

    const links: Record<string, any> = {};
    dag.links.forEach(l => {
      links[l.id] = {
        label: l.label,
        source: l.source,
        target: l.target,
        dataReference: l.dataReference ?? null
      };
    });

    const clusters = dag.clusters.map(c => ({
      id: c.id,
      label: c.label,
      childNodeIds: [...c.childNodeIds]
    }));

    const data = { ...dag.data };

    return { nodes, links, clusters, data };
  }

  private parseDisplay(value: any): NodeDisplay | null {
    if (!value || typeof value !== 'object') return null;
    const x = Number((value as any).x);
    const y = Number((value as any).y);
    const maxAbs = 1e6;
    if (!isFinite(x) || !isFinite(y)) return null;
    if (Math.abs(x) > maxAbs || Math.abs(y) > maxAbs) return null;
    return { x, y };
  }
}
