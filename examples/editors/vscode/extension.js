// Minimal Eventboat language-server extension (redesign-v3.md M4).
// It launches the eventboat binary in `lsp` mode and delegates everything
// (diagnostics, completion, hover) to it. Not published to the marketplace —
// see README.md for one-step local activation.
const vscode = require('vscode');
const {
  LanguageClient,
  TransportKind,
} = require('vscode-languageclient/node');

let client;

function activate(context) {
  const config = vscode.workspace.getConfiguration('eventboat');
  if (!config.get('enable', true)) {
    return;
  }
  const binary = config.get('path', 'eventboat');
  const serverOptions = {
    command: binary,
    args: ['lsp'],
    transport: TransportKind.stdio,
  };
  // YAML files: pipelines are single YAML documents; the server itself
  // ignores non-pipeline content cheaply (verify on an unrelated YAML file
  // produces at most one apiVersion/kind diagnostic).
  const clientOptions = {
    documentSelector: [{ scheme: 'file', language: 'yaml' }],
  };
  client = new LanguageClient(
    'eventboat',
    'Eventboat pipelines',
    serverOptions,
    clientOptions
  );
  client.start();
}

function deactivate() {
  return client ? client.stop() : undefined;
}

module.exports = { activate, deactivate };
