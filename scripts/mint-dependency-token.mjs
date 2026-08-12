// Mint a read-only GitHub App installation token for the two private CI
// dependencies. Reads APP_ID and APP_PRIVATE_KEY from the environment,
// resolves the app's launchapp-dev installation, and prints the token on
// stdout. No values other than the token are ever printed.
import crypto from "node:crypto";

const b64 = (obj) => Buffer.from(JSON.stringify(obj)).toString("base64url");

const now = Math.floor(Date.now() / 1000);
const header = b64({ alg: "RS256", typ: "JWT" });
const payload = b64({ iat: now - 60, exp: now + 540, iss: process.env.APP_ID });
const signature = crypto
  .sign("sha256", Buffer.from(`${header}.${payload}`), process.env.APP_PRIVATE_KEY)
  .toString("base64url");
const jwt = `${header}.${payload}.${signature}`;

const headers = {
  authorization: `Bearer ${jwt}`,
  accept: "application/vnd.github+json",
  "user-agent": "animus-env-railway-ci",
  "content-type": "application/json",
};

const installations = await (
  await fetch("https://api.github.com/app/installations", { headers })
).json();
const target = installations.find(
  (installation) => installation.account && installation.account.login === "launchapp-dev",
);
if (!target) {
  throw new Error("no launchapp-dev installation for the dependency app");
}

const mint = await fetch(
  `https://api.github.com/app/installations/${target.id}/access_tokens`,
  {
    method: "POST",
    headers,
    body: JSON.stringify({
      repositories: ["animus-environment-base", "animus-env-transport"],
      permissions: { contents: "read" },
    }),
  },
);
const body = await mint.json().catch(() => ({}));
if (mint.status !== 201 || !body.token) {
  throw new Error(`dependency token mint failed: ${mint.status} ${body.message || ""}`);
}
process.stdout.write(body.token);
