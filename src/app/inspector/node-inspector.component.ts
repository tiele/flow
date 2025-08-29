import { Component, EventEmitter, Input, OnChanges, Output, SimpleChanges } from '@angular/core';
import { DagLoaderService } from '../dag-loader.service';
import { RuntimeDag } from '../models';
import { DiffViewerComponent } from './diff-viewer.component';
import { NgIf, NgSwitch, NgSwitchCase, JsonPipe } from '@angular/common';
import { FormsModule } from '@angular/forms';

@Component({
  selector: 'app-node-inspector',
  standalone: true,
  imports: [DiffViewerComponent, NgIf, NgSwitch, NgSwitchCase, JsonPipe, FormsModule],
  templateUrl: './node-inspector.component.html',
  styleUrls: ['./node-inspector.component.scss']
})
export class NodeInspectorComponent implements OnChanges {
  @Input() dag?: RuntimeDag;
  @Input() selectedKey?: string;
  @Input() mode: 'full' | 'editor' | 'diff' = 'full';
  // When provided in editor mode, sets the overall editor container height
  // and the textarea will flex to fill remaining space
  @Input() containerHeight?: number;
  @Output() updateRule = new EventEmitter<{
    key: string;
    newContent: any;
    type?: string;
    category?: string;
    description?: string;
    reference?: string;
  }>();

  ruleText = '';
  nodeType = '';
  inPayload?: any;
  outPayload?: any;
  focusPaths?: string[];
  ruleReference = '';
  ruleCategory = '';
  ruleDescription = '';

  constructor(private loader: DagLoaderService) {}

  ngOnChanges(_: SimpleChanges) {
    if (!this.dag || !this.selectedKey) return;

    const n = this.dag.nodes.find(n => n.key === this.selectedKey)!;
    this.ruleText = n?.rule?.content ? JSON.stringify(n.rule.content, null, 2) : '';
    this.nodeType = n?.rule?.type || n.type || 'STEP';
    this.ruleReference = n?.rule?.reference || '';
    this.ruleCategory = n?.rule?.category || '';
    this.ruleDescription = n?.rule?.description || '';

    const io = this.loader.ioForNode(this.dag, this.selectedKey);
    this.inPayload = io.inPayload;
    this.outPayload = io.outPayload;

    // Choose focused paths for diff based on rule category
    const cat = n?.rule?.category || n?.rule?.type || '';
    this.focusPaths = computeFocusPaths(cat);
  }

  onSave() {
    try {
      const parsed = this.ruleText.trim() ? JSON.parse(this.ruleText) : null;
      this.updateRule.emit({
        key: this.selectedKey!,
        newContent: parsed,
        type: this.nodeType,
        category: this.ruleCategory,
        description: this.ruleDescription,
        reference: this.ruleReference
      });
    } catch {
      alert('Invalid JSON in rule editor.');
    }
  }
}

// Helper to scope diffs to relevant areas per category
export function computeFocusPaths(category: string): string[] | undefined {
  switch ((category || '').toUpperCase()) {
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
