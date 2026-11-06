# Security Policy

## Supported versions

Only the latest minor release is supported with security fixes. Upgrade to the
latest release before reporting an issue.

## Reporting a vulnerability

Report vulnerabilities through GitHub's private vulnerability reporting on this
repository (the **Security** tab → **Report a vulnerability**). Do not open a
public issue for a security problem.

Include what you can: the affected version, a description, and steps to
reproduce. You will get an acknowledgement, and a fix or a decision within a
reasonable time.

## Trust model

Storages are the trust boundary: the tool guards against a corrupted or
truncated object and against a storage the deployment cannot read, not against
someone who can write the storage.

- **The manifest is data, not a signature.** The size and sha256 of every
  stored object are recorded beside it in the run manifest, and a restore
  checks both before it applies anything. That catches a short download or a
  damaged object; it does not catch a deliberate replacement, because a
  manifest sits in the same storage as the bytes it describes. Whoever can
  write the prefix can supply both a new object and a manifest that matches it.
- **`age` seals, it does not authenticate the sender.** A sealed artifact cannot
  be read without the private identity, and an altered one fails to decrypt —
  but anyone holding the public recipient can seal new bytes that decrypt
  cleanly. Encryption is confidentiality, not proof of origin.
- **Restore trusts the chain the manifest records.** A restore runs the
  processors named in that manifest, never a pipeline looked up in the current
  configuration, so a rewritten manifest on a writable storage decides which
  processors run over the bytes.
- **Bound the credentials to the prefix and to the operations a run needs.**
  Scope them to the prefix the tool uses, and grant the reads the normal path
  requires: the preflight listing (`List`), upload verification and the survey
  (`Stat`), and the `Get` that reads manifests and restores objects. `Put` (its
  multipart calls included) and `Delete` complete the set; a write-only key
  fails the preflight and every restore. Enable bucket versioning or object
  lock: the size and sha256 check refuses a mismatched object, but it cannot
  give back the copy an overwrite or a delete took away.
- **A restore needs no privileged account.** A dump carries object owners and
  grants, so restoring as a superuser reproduces the source server's
  privileges. Give the restore role what the target needs to create and write
  its own database, and nothing more.

The image and the chart are signed keyless by this repository's CI; verify
either against the workflow identity named in
[README.md](README.md#docs-and-security).
