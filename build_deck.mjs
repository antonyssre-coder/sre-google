import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const projectDir = path.dirname(fileURLToPath(import.meta.url));
const input = path.join(projectDir, 'Kubernetes_Observability_Automation_5slides.pptx');
const tempOutput = path.join(os.tmpdir(), 'Kubernetes_Observability_Automation_5slides-rebuilt.pptx');
const home = process.env.HOME || process.env.USERPROFILE || os.homedir();
const artifactTool = path.join(
  home,
  '.cache', 'codex-runtimes', 'codex-primary-runtime', 'dependencies', 'node',
  'node_modules', '@oai', 'artifact-tool', 'dist', 'artifact_tool.mjs',
);
const { FileBlob, PresentationFile } = await import(pathToFileURL(artifactTool).href);
const presentation = await PresentationFile.importPptx(await FileBlob.load(input));

function setText(slideNumber, shapeId, value) {
  const slide = presentation.slides.items[slideNumber - 1];
  const shape = slide?.shapes.items.find((item) => String(item.id) === String(shapeId));
  if (!shape) throw new Error(`Could not find slide ${slideNumber}, shape ${shapeId}.`);
  shape.text = value;
}

function deleteShapeByName(slideNumber, name) {
  const slide = presentation.slides.items[slideNumber - 1];
  const shape = slide?.shapes.items.find((item) => item.name === name);
  if (shape) shape.delete();
}

if (presentation.slides.items.length !== 5) {
  throw new Error('The source presentation must contain the five selected slides.');
}

// Slide 1: architecture
setText(1, 2, 'GKE observability collector');
setText(1, 4, 'Python uses your kubeconfig, Metrics Server, and optional Prometheus and trace search APIs.');
setText(1, 7, 'GKE API\nInventory, events, logs');
setText(1, 8, 'Metrics Server\nCurrent CPU and memory');
setText(1, 9, 'Prometheus + traces\nOptional backend queries');
setText(1, 10, 'Python collector\nRead-only context');
setText(1, 11, 'Health summary\n80% CPU or memory');
setText(1, 12, 'JSON snapshot\nSignals and errors');
setText(1, 13, 'SMTP email\nConfigured SRE team');
setText(1, 6, '01');

// Slide 2: collection flow
setText(2, 2, 'Collect signals and check health');
setText(2, 4, 'Each source is queried independently; section errors stay visible in the snapshot.');
setText(2, 7, '1  Load cluster configuration and kubeconfig context\n2  Collect inventory, events, and bounded pod logs\n3  Read current CPU and memory; query optional Prometheus and trace APIs\n4  Check API, node, and pod health; compare node usage with threshold\n5  Write timestamped JSON; email the team if a configured threshold is breached');
setText(2, 8, 'Metrics Server is current usage; Prometheus provides history. Trace search is backend-specific.');
setText(2, 6, '02');

// Slide 3: GKE identity and RBAC
setText(3, 2, 'Use a GKE context with read-only permissions');
setText(3, 4, 'Run from a host that can reach the cluster API and configured telemetry endpoints.');
setText(3, 7, 'gcloud get-credentials\nTarget project and region');
setText(3, 8, 'Kubeconfig context\nGKE auth plugin');
setText(3, 9, 'RBAC reader\nObjects, logs, metrics');
setText(3, 12, 'Install gke-gcloud-auth-plugin if kubeconfig uses exec authentication.\nGrant cluster access through Google Cloud IAM, then bind the collector identity to rbac-readonly.yaml.\nConfirm pods/log and metrics.k8s.io access; Metrics Server must be available for CPU and memory.');
setText(3, 6, '03');

// Slide 4: utilization and threshold logic
setText(4, 2, 'A CPU or memory breach marks health degraded');
setText(4, 4, 'Default threshold: 80% inclusive, measured against node allocatable capacity.');
setText(4, 7, 'CPU % =\nused cores / allocatable cores x 100');
setText(4, 8, 'Memory % =\nused bytes / allocatable bytes x 100');
deleteShapeByName(4, 'Straight Arrow Connector 8');
setText(4, 10, 'Evaluate each node separately. Either metric at or above threshold creates a breach.\nThe summary also checks API reachability, node Ready state, and pod phase.\nWhen enabled, SMTP alerts repeat each run while a breach persists.');
setText(4, 6, '04');

// Slide 5: command and JSON output
setText(5, 2, 'Run once to create a JSON snapshot');
setText(5, 4, 'The output records cluster state, metrics, trace results, health, and collection errors.');
setText(5, 7, 'health and summary\nnodes, pods, deployments, services\nevents and bounded pod logs\nresource_metrics and threshold_breaches\nPrometheus and trace results\nerrors and collection timestamp');
setText(5, 8, 'Run once');
setText(5, 9, 'python k8s_observability.py --config cluster_config.yaml');
setText(5, 10, 'Override output and enable diagnostics');
setText(5, 11, 'python k8s_observability.py --config cluster_config.yaml --output output/prod --verbose');
setText(5, 12, 'Partial section failures are recorded in JSON; fatal setup errors exit with code 1.');
setText(5, 6, '05');

const pptx = await PresentationFile.exportPptx(presentation);
await fs.mkdir(path.dirname(tempOutput), { recursive: true });
await pptx.save(tempOutput);
console.log(`Built ${tempOutput} with ${presentation.slides.items.length} slides.`);
