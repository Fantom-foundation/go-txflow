# TxFlow

[Byzantine-Fault Tolerant](https://en.wikipedia.org/wiki/Byzantine_fault_tolerance)
[State Machines](https://en.wikipedia.org/wiki/State_machine_replication).
Or [Blockchain](https://en.wikipedia.org/wiki/Blockchain_(database)), for short.

TxFlow Core is asynchronous Byzantine Fault Tolerant (aBFT) middleware that takes a state transition machine - written in any programming language -
and securely replicates it on many machines.

## Releases

NOTE: The master branch is now an active development branch (starting with `v0.1.0`). Please, do not depend on it and
use [releases](https://github.com/Fantom-foundation/go-txflow/releases) instead.

In any case, if you intend to run TxFlow in production,
please [contact us](mailto:contact@fantom.foundation).

## Minimum requirements

Requirement|Notes
---|---
Go version | Go1.11.4 or higher

## Contributing

Please abide by the [Code of Conduct](CODE_OF_CONDUCT.md) in all interactions,
and the [contributing guidelines](CONTRIBUTING.md) when submitting code.

## Versioning

### Semantic Versioning

TxFlow uses [Semantic Versioning](http://semver.org/) to determine when and how the version changes.
According to SemVer, anything in the public API can change at any time before version 1.0.0

To provide some stability to Tendermint users in these 0.X.X days, the MINOR version is used
to signal breaking changes across a subset of the total public API. This subset includes all
interfaces exposed to other processes (cli, rpc, p2p, etc.), but does not
include the in-process Go APIs.

That said, breaking changes in the following packages will be documented in the
CHANGELOG even if they don't lead to MINOR version bumps:

- crypto
- types
- rpc/client
- config
- node
- libs
  - bech32
  - common
  - db
  - errors
  - log

Exported objects in these packages that are not covered by the versioning scheme
are explicitly marked by `// UNSTABLE` in their go doc comment and may change at any
time without notice. Functions, types, and values in any other package may also change at any time.

### Upgrades

In an effort to avoid accumulating technical debt prior to 1.0.0,
we do not guarantee that breaking changes (ie. bumps in the MINOR version)
will work with existing tendermint blockchains. In these cases you will
have to start a new blockchain, or write something custom to get the old
data into the new chain.
