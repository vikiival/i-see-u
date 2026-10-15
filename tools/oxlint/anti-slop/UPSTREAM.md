# Anti-slop provenance

Source repository: Unknown; this installation was copied from the local `install-anti-slop` skill bundle.

Source revision: Unknown. A recoverable pristine snapshot is retained at `.agents/skills/install-anti-slop/assets/anti-slop/`.

Installed paths:

- `tools/oxlint/anti-slop/index.ts`
- `tools/oxlint/anti-slop/rules/`
- `tools/oxlint/anti-slop/shared/`
- `tools/oxlint/anti-slop/effect/` (not registered; this repository has no direct `effect` dependency)
- `tools/oxlint/anti-slop/vendor/eslint-stylistic/`

Intentional deviations: none from the bundled snapshot. The generic plugin is registered as `anti-slop` in `oxlint.config.ts`; the optional Effect plugin remains disabled because `effect` is not declared directly by this package.
