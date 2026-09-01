# Riverpod Skills

Agent skills for [Riverpod](https://github.com/riverpod/riverpod), published for the [skills.sh](https://skills.sh/) ecosystem.

Install with the [Skills CLI](https://github.com/vercel-labs/skills):

```bash
# Install into the current project (Cursor, Claude Code, Codex, etc.)
npx skills add riverpod/riverpod@riverpod

# Install globally for all projects
npx skills add riverpod/riverpod@riverpod -g -y

# Try without installing
npx skills use riverpod/riverpod@riverpod
```

After publishing to GitHub, the skill page will appear at:

`https://skills.sh/riverpod/riverpod/riverpod`

## Skills in this directory

| Skill | Description |
|-------|-------------|
| [riverpod](riverpod/SKILL.md) | Author, validate, test, and run Riverpod DAG pipelines (YAML/HOCON, eql, plugins) |

## Layout

This repo follows the [Agent Skills](https://github.com/vercel-labs/skills) convention:

```
skills/
├── README.md           # this file
└── riverpod/
    ├── SKILL.md        # required — name must match directory
    └── reference.md    # plugin & config reference (bundled with skill)
```

The CLI discovers skills under `skills/<name>/SKILL.md` automatically.

## Development

When editing skills locally:

```bash
# Install from this repo (path to repo root or skills dir)
npx skills add ./ --skill riverpod

# Validate frontmatter: name matches directory, description present
npx skills init --help
```

See [docs/ai-agent.md](../docs/ai-agent.md) for the Agent integration roadmap.
