# Writing Agent Instructions

Keep a rule in `AGENTS.md` if most tasks need it, or if an agent could break it without noticing
it is in that situation. Move procedures and reference material to `.agents/`, behind a pointer
that starts with its trigger. When adding instructions to a directory that has no `AGENTS.md` or
`CLAUDE.md`, create `AGENTS.md` as the real file and add a `CLAUDE.md` symlink beside it
(`ln -s AGENTS.md CLAUDE.md`). After editing agent docs, run `dev/agent_docs_lint.py --strict`,
which CI also runs: it checks the pairing, that every pointer resolves and every `.agents/` file
has one, and Codex's 32 KiB budget per chain.
