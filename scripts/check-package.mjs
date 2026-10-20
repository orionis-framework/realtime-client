import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const root = new URL("../", import.meta.url);
const metadata = JSON.parse(await readFile(new URL("package.json", root), "utf8"));
const args = process.argv.slice(2);
assert.ok(args.length === 0 || (args.length === 1 && args[0] === "--write"), "Unknown arguments");
assert.notEqual(metadata.private, true, "The package must not be private");
assert.equal(metadata.type, "module");
assert.equal(metadata.exports["."].import, metadata.main);
assert.equal(metadata.exports["."].types, metadata.types);
assert.equal(Object.keys(metadata.exports["."])[0], "types");
assert.equal(metadata.publishConfig.access, "public");
assert.equal(metadata.publishConfig.registry, "https://registry.npmjs.org/");
assert.equal(Object.keys(metadata.dependencies ?? {}).length, 0, "Runtime dependencies are not expected");
assert.ok(process.env.npm_execpath, "Run this check through npm run integrity or npm run integrity:check");

const report = JSON.parse(execFileSync(process.execPath, [
    process.env.npm_execpath, "pack", fileURLToPath(root),
    "--dry-run", "--ignore-scripts", "--json",
], { cwd: fileURLToPath(root), encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }));
const archive = Array.isArray(report) ? report[0] : report[metadata.name];
assert.ok(archive, "npm did not return an archive manifest");
assert.equal(archive.name, metadata.name);
assert.equal(archive.version, metadata.version);
assert.deepEqual(archive.bundled, []);
const paths = archive.files.map(entry => entry.path).sort();
assert.deepEqual(paths, [...new Set(["package.json", ...metadata.files])].sort(),
    "The npm archive must contain only the declared distribution files");

const files = [];
for (const path of paths) {
    if (path === "integrity.json") continue;
    const content = await readFile(new URL(path, root));
    files.push({
        path,
        bytes: content.byteLength,
        sha256: createHash("sha256").update(content).digest("hex"),
    });
}
const integrity = {
    format: 1,
    algorithm: "sha256",
    package: metadata.name,
    version: metadata.version,
    files,
};
const destination = new URL("integrity.json", root);
if (args.includes("--write")) {
    await writeFile(destination, JSON.stringify(integrity, null, 2) + "\n", "utf8");
} else {
    assert.deepEqual(JSON.parse(await readFile(destination, "utf8")), integrity,
        "Integrity data is outdated; run npm run integrity");
}
console.log(`OK: ${metadata.name}@${metadata.version}, ${paths.length} published files, SHA-256 integrity.`);
