// Build the GitHub Release assets for animus-environment-railway.
//
// `animus install { git, tag }` resolves a release asset by the runner's target
// triple (current_target_triple() -> e.g. x86_64-unknown-linux-gnu), verifies it
// against a per-asset `<asset>.sha256` sidecar, extracts, and execs the contained
// binary. This mirrors the Rust-plugin convention proven by the working
// animus-workflow-runner-default releases (and the animus-postgres release fix):
//
//   <name>-<version>-<target>.tar.gz          (the archive)
//   <name>-<version>-<target>.tar.gz.sha256   (per-asset checksum sidecar)
//
//   e.g. animus-environment-railway-v0.1.0-x86_64-unknown-linux-gnu.tar.gz (+ .sha256)
//
// animus-environment-railway bundles (esbuild) to a single self-contained JS
// file with a node shebang -- it inlines animus-env-transport,
// animus-environment-base, the SDK, ws and zod -- so ONE build serves every
// triple (no per-arch matrix) and installs with no node_modules. Every triple
// asset holds the same bytes. In addition to any requested triples we always
// publish a platform-independent `-noarch` asset -- the ao-cli install resolver
// (select_release_asset) falls back to a `-noarch` / `-any` asset when no
// triple-specific asset matches the host, so the noarch archive makes the
// plugin installable on any platform.
//
// This script produces, under dist/release/:
//   <name>-<version>-<target>.tar.gz          (one per target, incl. noarch)
//   <name>-<version>-<target>.tar.gz.sha256   (one sidecar per archive)
//
// It does NOT cut a release. It prints the `gh release create` command to run by
// hand once the assets look right.
//
// Usage:
//   node scripts/release.mjs                 # default target + noarch
//   node scripts/release.mjs <triple> ...    # extra triples share the same bytes
//   ANIMUS_RELEASE_TARGETS=a,b node scripts/release.mjs
//   ANIMUS_RELEASE_SKIP_INSTALL=1 node scripts/release.mjs   # skip `npm ci`

import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { mkdirSync, readFileSync, rmSync, writeFileSync, statSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, '..');
const distDir = join(root, 'dist');
const releaseDir = join(distDir, 'release');
const binName = 'animus-environment-railway';
const bin = join(distDir, binName);

// The platform-independent fallback target. ao-cli's select_release_asset picks
// a triple-specific asset first, then falls back to a `-noarch` asset -- so the
// JS bundle (which runs anywhere node 20+ runs) is always installable.
const NOARCH_TARGET = 'noarch';
const DEFAULT_TRIPLES = ['x86_64-unknown-linux-gnu'];

function requestedTriples() {
  const fromArgs = process.argv.slice(2).filter((a) => !a.startsWith('-'));
  if (fromArgs.length > 0) return [...new Set(fromArgs)];
  const fromEnv = (process.env.ANIMUS_RELEASE_TARGETS ?? '')
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
  if (fromEnv.length > 0) return [...new Set(fromEnv)];
  return DEFAULT_TRIPLES;
}

// Always append noarch as the universal fallback (deduped in case a caller
// passes it explicitly).
function targets() {
  return [...new Set([...requestedTriples(), NOARCH_TARGET])];
}

function run(cmd, args) {
  process.stdout.write(`$ ${cmd} ${args.join(' ')}\n`);
  execFileSync(cmd, args, { cwd: root, stdio: 'inherit' });
}

function sha256(path) {
  return createHash('sha256').update(readFileSync(path)).digest('hex');
}

const pkg = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));
const version = pkg.version;
const tag = `v${version}`;

// 1. deps (skippable) + 2. bundle -> dist/animus-environment-railway
if (!['1', 'true', 'yes'].includes((process.env.ANIMUS_RELEASE_SKIP_INSTALL ?? '').toLowerCase())) {
  run('npm', ['ci']);
}
run('npm', ['run', 'bundle']);

statSync(bin); // fail loudly if the bundle is missing

// 3. package one tarball per target (same bundle bytes) + a per-asset sidecar.
rmSync(releaseDir, { recursive: true, force: true });
mkdirSync(releaseDir, { recursive: true });

const assets = [];
for (const target of targets()) {
  const archive = `${binName}-${tag}-${target}.tar.gz`;
  // Archive just the executable at the archive root so extraction yields an
  // exec-ready `animus-environment-railway`. Portable flags kept off for
  // cross-platform tar compatibility; the bundle is already chmod 0755.
  run('tar', ['-czf', join(releaseDir, archive), '-C', distDir, binName]);

  // Per-asset `<archive>.sha256` sidecar in the `<hex>  <filename>` shape the
  // ao-cli resolver (parse_sha256_sidecar) reads the leading hex digest from.
  const sidecar = `${archive}.sha256`;
  const hex = sha256(join(releaseDir, archive));
  writeFileSync(join(releaseDir, sidecar), `${hex}  ${archive}\n`, 'utf8');

  assets.push(archive, sidecar);
}

process.stdout.write('\nReleased assets staged in dist/release/:\n');
for (const a of assets) process.stdout.write(`  ${a}\n`);

process.stdout.write(
  `\nTo publish (run by hand):\n` +
    `  gh release create ${tag} \\\n` +
    assets.map((a) => `    dist/release/${a} \\\n`).join('') +
    `    --repo launchapp-dev/animus-environment-railway --title ${tag} --notes "animus-environment-railway ${tag}"\n`,
);
