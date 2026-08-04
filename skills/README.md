# EdgeStream Skills

Agent skills for [EdgeStream](https://github.com/edgesets/edgestream), published for the [skills.sh](https://skills.sh/) ecosystem.

Install with the [Skills CLI](https://github.com/vercel-labs/skills):

```bash
# Install into the current project (Cursor, Claude Code, Codex, etc.)
npx skills add edgesets/edgestream@edgestream

# Install globally for all projects
npx skills add edgesets/edgestream@edgestream -g -y

# Try without installing
npx skills use edgesets/edgestream@edgestream
```

After publishing to GitHub, the skill page will appear at:

`https://skills.sh/edgesets/edgestream/edgestream`

## Skills in this directory

| Skill | Description |
|-------|-------------|
| [edgestream](edgestream/SKILL.md) | Author, validate, test, and run EdgeStream DAG pipelines (YAML/HOCON, eql, plugins) |

## Layout

This repo follows the [Agent Skills](https://github.com/vercel-labs/skills) convention:

```
skills/
├── README.md           # this file
└── edgestream/
    ├── SKILL.md        # required — name must match directory
    └── reference.md    # plugin & config reference (bundled with skill)
```

The CLI discovers skills under `skills/<name>/SKILL.md` automatically.

## Development

When editing skills locally:

```bash
# Install from this repo (path to repo root or skills dir)
npx skills add ./ --skill edgestream

# Validate frontmatter: name matches directory, description present
npx skills init --help
```

See [docs/ai-agent.md](../docs/ai-agent.md) for the Agent integration roadmap.
