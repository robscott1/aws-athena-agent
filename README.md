# Athena Query Tool

A single place to interact with Athena and make educated, complex queries with AI assistance.

## Why This Exists

Executing queries through Python (instead of the Athena console) lets Claude Code:

- **Write queries on the fly** - Describe your investigation, Claude builds the SQL
- **Learn schemas dynamically** - `DESCRIBE table` results get committed to `schema.txt` for future reference
- **Analyze results and iterate** - Claude reads output, suggests next steps, refines queries
- **Build institutional knowledge** - Save useful queries to `queries/`, commit learnings to the repo

The workflow becomes: describe problem → Claude queries → analyze → iterate → solve → commit what you learned.

This beats copy-pasting between the Athena console and ChatGPT. Everything stays in one place, queries are version controlled, and the AI learns your schema over time.

### Data Security Warning

Query results in `output/` contain data and are **gitignored**. Do not commit query results or include sensitive information in committed files (queries, docs, schema.txt).

What's safe to commit:
- Query templates with `$parameter` placeholders
- Schema definitions (column names/types, not data)
- Investigation methodology docs

What's NOT safe to commit:
- Query output files
- Queries with hardcoded IDs, device IDs, etc.
- Screenshots of results

---

## Setup

### 1. Install AWS CLI

If you don't have it:
```bash
brew install awscli
```

### 2. Authenticate with SSO

```bash
aws configure sso
```

When prompted:
- SSO start URL: your organization's SSO start URL
- SSO Region: your AWS region
- Role: your assigned role
- Profile name: `your-profile` (or your preference)

Then login:
```bash
aws sso login --profile your-profile
```

### 3. Install Dependencies

From the `scripts/` root:
```bash
poetry install
```

## Usage with Claude Code

This tool is designed to be used with Claude Code. I use PyCharm with the Claude Code extension.

### Getting Started

Sometimes it's hard to get started on a new investigation. Tips:

1. **Start small** - Begin with a known working query or a simple `SELECT * FROM table LIMIT 10`
2. **Get data returning first** - Validate the table exists and you have access before adding filters
3. **Iterate** - Add filters one at a time to narrow down results

### Scrutinize Every Query

Claude needs your permission to run queries. Before approving, verify:

- [ ] **Has a `LIMIT`** - Results over a few hundred rows are useless to both you and Claude
- [ ] **Filters on `dt` partition** - Always include `dt >= 'YYYY-MM-DD'` to avoid full table scans
- [ ] **Reasonable date range** - Default to 2 days unless you need more

If the result is too large, it's useless. You can't read it, Claude can't analyze it, and you're burning Athena credits.

### Reference CLAUDE.md

If Claude is producing bad queries or forgetting setup, remind it:

> "Check CLAUDE.md for query guidelines"

The `CLAUDE.md` file contains instructions for:
- **Query hygiene** - LIMIT, dt partitions, read-only validation
- **Database setup** - Region, database name, workgroup
- **S3 bucket destination** - Where results are stored

### Database Configuration

Different projects use different Athena setups. Pay attention to:

| Setting | Where to check |
|---------|----------------|
| `AWS_REGION` | us-east-1, eu-west-1, etc. |
| `DATABASE` | events, device_logs, shopfully_production, etc. |
| `WORKGROUP` | primary, or project-specific |
| `S3_OUTPUT` | Must match the region |

If you're unsure, copy a working setup from the Athena console for that project.

## Files

```
athena/
├── query.py           # Main executor - edit QUERY and PARAMS, then run
├── schema.txt         # Table schemas (append new discoveries)
├── CLAUDE.md          # Instructions for Claude Code
├── queries/           # Reusable query templates
└── output/            # Query results (timestamped .txt files)
```

## Running a Query

1. Edit `query.py`:
   - Set `AWS_REGION`, `DATABASE`, `WORKGROUP`, `S3_OUTPUT`
   - Set `QUERY` (inline SQL or path to .sql file)
   - Set `PARAMS` dict with substitution values

2. Run:
   ```bash
   poetry run python athena/query.py
   ```

3. Results print to stdout and save to `output/query_YYYYMMDD_HHMMSS.txt`

## Example Workflow

```
You: "Why is account acme-corp seeing elevated error rates on /api/export?"

Claude: [queries error_logs, finds spike on specific endpoint]
Claude: [cross-references api_requests for pattern]
Claude: "Timeouts spiking after deploy at 14:00 — likely a regression in the export handler"
```
