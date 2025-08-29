export interface Rule {
  id: number;
  type: string;
  category: string;
  description?: string;
  content?: any;
  reference?: string;
}
export interface DagNode {
  key: string;
  label: string;
  type?: string | null;
  inputs?: string[];
  outputs?: string[];
  rule?: Rule | null;
}
export interface DagLink {
  id: string;
  label?: string;
  source: string;
  target: string;
  dataReference?: string | null;
}
export interface Cluster {
  id: string;
  label?: string;
  childNodeIds: string[];
}
export interface DagFile {
  nodes: Record<string, any>;
  links: Record<string, any>;
  clusters?: Cluster[];
  data?: Record<string, any>;
}
export interface RuntimeDag {
  nodes: DagNode[];
  links: DagLink[];
  clusters: Cluster[];
  data: Record<string, any>;
}
export type NodeIO = { inPayload?: any; outPayload?: any; };
