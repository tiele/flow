# Flow Viewer Architecture

## Purpose and Scope
Flow Viewer is an Angular 17 standalone application for exploring directed acyclic graphs (DAGs) that describe business rule flows. It loads DAG definitions from JSON files, visualises the flow with Cytoscape, and lets users inspect, edit, and diff node-level rules and payloads.

## High-Level Structure
- `src/main.ts` bootstraps the root `AppComponent` with the standalone configuration found in `src/app/app.config.ts`.
- `src/app/` contains all application code: the root component, a DAG loader service, graph visualisation components, inspector/editor components, data models, and type declarations.
- `src/assets/` ships sample DAG exports (`output_poseidon.json`) and reference artefacts (BPMN, SQL, YAML) used for testing and demos.
- `angular.json`, `tsconfig*.json`, and `package.json` define the Angular workspace, TypeScript configuration, and dependencies.

## Runtime Flow
1. **Bootstrap** – `main.ts` invokes `bootstrapApplication(AppComponent, appConfig)` to start the app with Router/Animations/Http providers but no routes.
2. **Data ingestion** – `AppComponent` uses `DagLoaderService` to load either the bundled sample JSON (`assets/output_poseidon.json`) or a user-provided file. The service normalises raw DAG JSON into the strongly typed `RuntimeDag` structure (`models.ts`).
3. **Presentation** – `AppComponent` hands the DAG to `FlowGraphComponent` for rendering and to `NodeInspectorComponent` / `DiffViewerComponent` for inspection. Component state tracks the currently selected node and synchronises editor/diff payloads.
4. **Interaction** – Selecting a node in the graph emits `selectNode`, which the root component handles to populate rule metadata, IO payloads, and focused diff paths for the inspector.
5. **Editing** – Saving changes in the inspector updates the in-memory `RuntimeDag`, immediately reflecting rule edits and payload previews.

## Core Modules
### AppComponent (`src/app/app.component.ts`)
- Hosts the page layout: header filters, graph pane, resizable inspector/diff split-view.
- Holds the active `RuntimeDag`, selected node key, and derived IO payloads.
- Delegates DAG loading to `DagLoaderService` and orchestrates communication between graph and inspector components.

### DagLoaderService (`src/app/dag-loader.service.ts`)
- Fetches and parses DAG definitions from files or URLs.
- Normalises nodes, links, clusters, and data payloads into runtime models.
- Exposes helper methods `refsForNode` and `ioForNode` to resolve the relevant IN/OUT data references for a node.

### FlowGraphComponent (`src/app/graph/flow-graph.component.ts`)
- Wraps Cytoscape with the dagre layout plugin to render the DAG with left-to-right flow.
- Builds Cytoscape elements from `RuntimeDag` nodes, links, and clusters, tagging nodes with rule metadata (type, category, global flag) used for styling.
- Applies custom styles for node categories and a badge overlay for `GLOBAL-*` nodes.
- Handles viewport resize via `ResizeObserver` and positions isolated nodes to keep them visible after layout.
- Emits `selectNode` when a non-cluster node is tapped.

### NodeInspectorComponent (`src/app/inspector/node-inspector.component.ts`)
- Provides three display modes (`full`, `editor`, `diff`); the app uses `editor` mode alongside the global diff panels.
- Mirrors rule metadata fields (reference, type, category, description) and JSON rule content for the selected node.
- Uses `DagLoaderService.ioForNode` to retrieve IN/OUT payloads and to derive focused diff paths by rule category.
- Emits `updateRule` events with parsed JSON content, allowing the root component to mutate the DAG in memory.

### DiffViewerComponent (`src/app/inspector/diff-viewer.component.ts`)
- Wraps `jsondiffpatch` to produce both HTML tree diffs and a custom side-by-side table view.
- Supports filtering changes by type (added/changed/removed) and optional path scoping (driven by focus paths supplied by the inspector/app components).
- Normalises complex JSON for stable previews and exposes expand/collapse controls for nested structures.

### Models and Types (`src/app/models.ts`, `src/app/types/cytoscape-dagre.d.ts`)
- `models.ts` defines the shared TypeScript interfaces (`Rule`, `DagNode`, `DagLink`, `Cluster`, `RuntimeDag`).
- The `.d.ts` file augments typings for the `cytoscape-dagre` plugin to satisfy the TypeScript compiler.

## Visualisation Pipeline
- Cytoscape establishes the graph canvas; dagre layout arranges nodes horizontally with configurable spacing.
- Cluster support: cluster DAG nodes become Cytoscape compound nodes (`$node > node` selectors) with dashed borders and padding.
- Node styling is data-driven via Cytoscape selectors keyed by rule type (`ntype`) and global flag badges encoded as inline SVG backgrounds.
- The component re-runs the layout whenever the DAG input changes and ensures isolated nodes remain visible by repositioning them relative to the viewport.

## Rule Inspection & Diffing
- The inspector displays rule metadata alongside a JSON editor bound to the selected node's rule content.
- `focusPaths` are computed from rule categories to emphasise relevant fields (e.g., `usage.product` for PRICING rules) during diffing.
- The diff viewer compares IN vs. OUT payloads, offering toggles between a tabular summary and the raw HTML tree produced by `jsondiffpatch`.
- Updates emitted from the inspector propagate back into `RuntimeDag`, so subsequent IO previews and diffs use the latest edits.

## Assets & Sample Data
- `src/assets/output_poseidon.json` is the default DAG sample loaded via the "Load sample" button.
- Additional BPMN diagrams, SQL extracts, and YAML definitions serve as reference material for understanding the broader business flow but are not loaded automatically.

## External Dependencies
- **Angular 17 standalone** for application structure, forms, and change detection.
- **Cytoscape 3.28** plus **cytoscape-dagre** for graph rendering and layout.
- **jsondiffpatch** for JSON diff computation and HTML formatting.

## Extensibility Notes
- Routing is already provisioned via `provideRouter([])`; adding multiple views would primarily involve supplying route configurations.
- Persisting rule edits or integrating with back-end services would go through `DagLoaderService`, which currently operates purely in-memory.
- Filtering by source type or flow is UI-only today; hook these controls into graph/inspector inputs to narrow displayed data.
