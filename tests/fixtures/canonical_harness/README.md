# EXharness workflow integration fixture

These are real EXharness runtime templates, copied byte-for-byte from
`skills/long-running-project-harness/references/scripts/`. They are test data,
not a second maintained runtime or the scripts deployed to production.
`source-manifest.json` records their provenance and SHA256 values.

`test_canonical_closeout.py` checks the hashes, substitutes the normal harness
path placeholders in a temporary project, then runs the actual shell/Python
scripts through Coordinate. The subset supports assignment, acceptance,
blocker/unblock, closeout, review-result, state refresh and receipt completion. It deliberately
does not replace script execution with a successful mock result.

The blocker template's additional provenance is recorded separately in the manifest.
Refresh templates from their canonical source when updating the supported EXharness
contract, preserving source hashes and rerunning the integration tests. Use
`EXHARNESS_SOURCE` parity checks during companion releases rather than silently
updating this fixture to satisfy a failing test.
