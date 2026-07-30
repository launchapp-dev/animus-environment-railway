import { build } from 'esbuild';
import { chmodSync, mkdirSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, '..');
const outfile = join(root, 'dist', 'animus-environment-railway');
const expectedTransportVersion = '0.3.2';
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
