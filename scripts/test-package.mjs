import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { copyFile, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const metadata = JSON.parse(await readFile(join(root, "package.json"), "utf8"));
assert.ok(Array.isArray(metadata.files), "Distribution files must be declared explicitly");
const expectedPaths = [...new Set(["package.json", ...metadata.files])].sort();
const expectedExports = [
    "AbortError", "ConnectionError", "LimitError", "ProtocolError", "RealtimeClient",
    "RealtimeError", "RemoteError", "TimeoutError",
].sort();

assert.equal(process.argv.length, 2, "This check does not accept arguments");
assert.ok(process.env.npm_execpath, "Run this check through npm run test:package");
assert.equal(metadata.name, "@orionis-framework/realtime-client");
assert.notEqual(metadata.private, true, "The package must be publishable");
assert.equal(metadata.publishConfig.access, "public");
assert.equal(metadata.publishConfig.registry, "https://registry.npmjs.org/");

function run(command, args, cwd) {
    try {
        return execFileSync(command, args, {
            cwd, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"],
            timeout: 60000,
        });
    } catch (error) {
        process.stderr.write(error.stdout ?? "");
        process.stderr.write(error.stderr ?? "");
        throw error;
    }
}

function npm(args, cwd) {
    return run(process.execPath, [process.env.npm_execpath, ...args], cwd);
}

async function filesIn(directory, prefix = "") {
    const paths = [];
    for (const entry of await readdir(directory, { withFileTypes: true })) {
        const path = prefix + entry.name;
        if (entry.isDirectory()) {
            paths.push(...await filesIn(join(directory, entry.name), path + "/"));
        } else {
            assert.ok(entry.isFile(), `Unexpected non-file in installed package: ${path}`);
            paths.push(path);
        }
    }
    return paths.sort();
}

const temporary = await mkdtemp(join(tmpdir(), "orionis-package-"));
try {
    const report = JSON.parse(npm([
        "pack", root, "--ignore-scripts", "--offline", "--json", "--pack-destination", temporary,
    ], root));
    const archive = Array.isArray(report) ? report[0] : report[metadata.name];
    assert.ok(archive, "npm did not return an archive manifest");
    assert.equal(archive.name, metadata.name);
    assert.equal(archive.version, metadata.version);
    assert.deepEqual(archive.bundled, []);
    assert.deepEqual(archive.files.map(entry => entry.path).sort(), expectedPaths);

    const tarball = join(temporary, archive.filename);
    const compressed = await readFile(tarball);
    assert.equal(compressed.byteLength, archive.size);
    assert.equal(createHash("sha1").update(compressed).digest("hex"), archive.shasum);
    assert.equal("sha512-" + createHash("sha512").update(compressed).digest("base64"), archive.integrity);

    const consumer = join(temporary, "consumer");
    await mkdir(consumer);
    await writeFile(join(consumer, "package.json"), JSON.stringify({ private: true, type: "module" }));
    npm([
        "install", tarball, "--ignore-scripts", "--no-audit", "--no-fund",
        "--package-lock=false", "--offline",
    ], consumer);

    // Inspect npm's extraction of the real archive, never the source working tree.
    const installed = join(consumer, "node_modules", ...metadata.name.split("/"));
    assert.deepEqual(await filesIn(installed), expectedPaths);
    const packedMetadata = JSON.parse(await readFile(join(installed, "package.json"), "utf8"));
    assert.deepEqual(packedMetadata, metadata);
    assert.equal(packedMetadata.type, "module");
    assert.equal(Object.keys(packedMetadata.exports["."])[0], "types");
    assert.equal(packedMetadata.exports["."].types, packedMetadata.types);
    assert.equal(packedMetadata.exports["."].import, packedMetadata.main);
    assert.deepEqual(packedMetadata.dependencies ?? {}, {});

    for (const path of expectedPaths) {
        const content = await readFile(join(installed, path));
        assert.equal(content.byteLength, archive.files.find(entry => entry.path === path).size);
        assert.deepEqual(content, await readFile(join(root, path)), "Published file differs from source: " + path);
    }

    await copyFile(join(installed, "orionis-realtime.js"), join(consumer, "standalone.mjs"));
    await writeFile(join(consumer, "smoke.mjs"), `
import assert from "node:assert/strict";
import * as api from ${JSON.stringify(metadata.name)};
import { pathToFileURL } from "node:url";
import { join } from "node:path";
assert.deepEqual(Object.keys(api).sort(), ${JSON.stringify(expectedExports)});
for (const name of ${JSON.stringify(expectedExports)}) assert.equal(typeof api[name], "function");
for (const name of ${JSON.stringify(expectedExports.filter(name => !["RealtimeClient", "RemoteError"].includes(name)))}) {
    assert.ok(new api[name]() instanceof Error);
    assert.ok(new api[name]() instanceof api.RealtimeError);
}
const remote = new api.RemoteError("test", "remote failure");
assert.ok(remote instanceof api.RealtimeError);
assert.equal(remote.code, "test");
const client = new api.RealtimeClient("wss://example.invalid/realtime", {
    webSocketFactory() { throw new Error("Construction must not open a connection"); },
});
assert.equal(client.state, "idle");
assert.equal(client.connectionId, null);
await client.dispose();
assert.equal(client.state, "disposed");
await assert.rejects(client.connect(), error => error instanceof api.ConnectionError && error.code === "disposed");
const direct = await import(pathToFileURL(join(${JSON.stringify(installed)}, "orionis-realtime.js")).href);
for (const name of ${JSON.stringify(expectedExports)}) assert.equal(direct[name], api[name]);
const standalone = await import("./standalone.mjs");
assert.deepEqual(Object.keys(standalone).sort(), ${JSON.stringify(expectedExports)});
for (const name of ${JSON.stringify(expectedExports)}) assert.equal(typeof standalone[name], "function");
const standaloneClient = new standalone.RealtimeClient("wss://example.invalid/realtime");
assert.equal(standaloneClient.state, "idle");
await standaloneClient.dispose();
`);
    run(process.execPath, ["smoke.mjs"], consumer);

    // Reuse the complete type suite against the installed package.
    await copyFile(join(root, "test", "types.ts"), join(consumer, "types.ts"));
    await copyFile(join(root, "test", "tsconfig.json"), join(consumer, "tsconfig.json"));
    const compiler = join(root, "node_modules", "typescript", "bin", "tsc");
    run(process.execPath, [compiler, "-p", "tsconfig.json"], consumer);
    run(process.execPath, [compiler, "-p", "tsconfig.json", "--module", "ESNext", "--moduleResolution", "Bundler"], consumer);

    console.log(`OK: real npm archive, ${expectedPaths.length} files, SHA-512 integrity, offline installation, ESM and TypeScript consumers.`);
} finally {
    assert.equal(dirname(resolve(temporary)), resolve(tmpdir()), "Temporary cleanup must stay inside the OS temp directory");
    assert.ok(basename(temporary).startsWith("orionis-package-"), "Unexpected cleanup target");
    await rm(temporary, { recursive: true, force: true, maxRetries: 3 });
}
