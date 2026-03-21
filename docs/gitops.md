# GitOps

DAPOS versions all pipeline configurations and connector code in a dedicated git repository, enabling version history, diff review, and point-in-time restore.

---

## How It Works

A separate git repository (not the DAPOS application repo) stores:

```
{PIPELINE_REPO_PATH}/
├── pipelines/
│   ├── demo-ecommerce-orders.yaml
│   ├── demo-stripe-charges.yaml
│   └── ...
├── connectors/
│   ├── mysql-source.py
│   ├── postgresql-target.py
│   └── ...
├── README.md (auto-generated)
└── .git/
```

### Auto-Commit on Every Mutation

When you create, update, pause, resume, or approve a pipeline, DAPOS automatically:

1. Writes the pipeline YAML to `pipelines/{name}.yaml`
2. Writes connector code to `connectors/{name}.py`
3. Commits with a descriptive message (e.g., "Update demo-stripe-charges: schedule changed")
4. Pushes to remote (if configured)

---

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `PIPELINE_REPO_PATH` | `./pipeline-repo` | Local directory for the git repo |
| `PIPELINE_REPO_REMOTE` | (none) | Remote URL (GitHub/GitLab) for push/pull |
| `PIPELINE_REPO_BRANCH` | `main` | Branch to use |
| `GITOPS_AUTO_PUSH` | `true` | Auto-push after each commit |
| `GITOPS_AUTO_PULL` | `true` | Auto-pull before each commit |

---

## API Endpoints

### Repository Status
```
GET /api/gitops/status
```
Returns: branch, last commit SHA, clean/dirty state, remote URL.

### Commit Log
```
GET /api/gitops/log?limit=20
```
Returns: recent commits with SHA, message, author, timestamp.

### Diff Between Commits
```
GET /api/gitops/diff?from={sha}&to={sha}
```
Returns: file-level diff between two commits.

### File at Commit
```
GET /api/gitops/file?path=pipelines/demo-stripe-charges.yaml&commit={sha}
```
Returns: file content at a specific commit.

### Pipeline History
```
GET /api/gitops/pipelines/{pipeline_id}/history?limit=20
```
Returns: commit history for a specific pipeline's YAML file.

### Restore from History
```
POST /api/gitops/restore?pipeline_id={id}&commit={sha}&dry_run=true
```
Restores a pipeline to a previous version. Use `dry_run=true` (default) to preview changes.

---

## Workflow Examples

### Review Pipeline Changes
```bash
# See what changed recently
curl http://localhost:8100/api/gitops/log?limit=5

# Diff between two versions
curl "http://localhost:8100/api/gitops/diff?from=abc123&to=def456"
```

### Rollback a Pipeline
```bash
# Find the commit to roll back to
curl http://localhost:8100/api/gitops/pipelines/demo-stripe-charges/history

# Preview the rollback
curl -X POST "http://localhost:8100/api/gitops/restore?pipeline_id=pipe-123&commit=abc123&dry_run=true"

# Execute the rollback
curl -X POST "http://localhost:8100/api/gitops/restore?pipeline_id=pipe-123&commit=abc123&dry_run=false"
```

### Multi-Developer Workflow
```bash
# Remote repo enables team collaboration
PIPELINE_REPO_REMOTE=git@github.com:team/pipeline-configs.git

# Developer A makes changes via DAPOS UI → auto-committed and pushed
# Developer B pulls changes and sees what changed
```

---

## Conflict Resolution

When two DAPOS instances modify the same pipeline simultaneously:

1. Auto-pull detects divergence
2. Auto-rebase for non-overlapping changes
3. For true conflicts: DAPOS is source of truth — force-writes its version and recommits

The git history preserves both versions, so nothing is lost.

---

## Backup with GitOps

The GitOps repo serves as a continuous backup:

```bash
# Clone for offline backup
git clone /path/to/pipeline-repo /backups/

# Restore entire platform from backup
POST /api/gitops/restore?dry_run=false
```
