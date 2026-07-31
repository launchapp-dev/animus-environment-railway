import { build } from 'esbuild';
import { chmodSync, mkdirSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, '..');
const outfile = join(root, 'dist', 'animus-environment-railway');
const expectedTransportVersion = '0.3.4';
const expectedEnvironmentBaseCommit = '56a370a2bf622ef009e90f2004353b89f8a7ad4c';
const transportPackage = JSON.parse(
  readFileSync(
    join(root, 'node_modules', '@launchapp-dev', 'animus-env-transport', 'package.json'),
    'utf8',
  ),
);
if (transportPackage.version !== expectedTransportVersion) {
  throw new Error(
    `refusing to bundle animus-environment-railway with animus-env-transport ${String(transportPackage.version)}; expected ${expectedTransportVersion}`,
  );
}

const lock = JSON.parse(readFileSync(join(root, 'package-lock.json'), 'utf8'));
const environmentBaseLock = lock.packages?.['node_modules/@launchapp-dev/animus-environment-base'];
if (
  typeof environmentBaseLock?.resolved !== 'string' ||
  !environmentBaseLock.resolved.endsWith(`#${expectedEnvironmentBaseCommit}`)
) {
  throw new Error(
    `refusing to bundle animus-environment-railway without animus-environment-base commit ${expectedEnvironmentBaseCommit}`,
  );
}

mkdirSync(join(root, 'dist'), { recursive: true });

await build({
  entryPoints: [join(root, 'src', 'index.ts')],
  outfile,
  bundle: true,
  platform: 'node',
  format: 'esm',
  target: 'node20',
  external: ['bufferutil', 'utf-8-validate'],
  banner: {
    js: "import { createRequire as __createRequire } from 'module'; const require = __createRequire(import.meta.url);",
  },
  logLevel: 'info',
});

chmodSync(outfile, 0o755);
console.log(`built ${outfile}`);
