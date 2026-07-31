export function assertReleaseManifest(rawManifest, { expectedName, expectedVersion }) {
  let manifest;
  try {
    manifest = JSON.parse(rawManifest);
  } catch (error) {
    throw new Error(`refusing to release a bundle with an invalid JSON manifest: ${String(error)}`);
  }

  if (manifest === null || typeof manifest !== 'object' || Array.isArray(manifest)) {
    throw new Error('refusing to release a bundle whose manifest is not a JSON object');
  }
  if (manifest.name !== expectedName) {
    throw new Error(
      `refusing to release bundle ${String(manifest.name)}; expected plugin name ${expectedName}`,
    );
  }
  if (manifest.version !== expectedVersion) {
    throw new Error(
      `refusing to release ${expectedName}@${String(manifest.version)} as ${expectedVersion}`,
    );
  }

  return manifest;
}
