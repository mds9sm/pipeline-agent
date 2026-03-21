# Disaster Recovery

DAPOS provides multiple mechanisms for recovering from failures: GitOps restore, crash recovery, and stale run cleanup.

---

## GitOps Restore

If the database is lost or corrupted, restore pipeline configurations from the GitOps repository:

```
POST /api/gitops/restore?dry_run=true
```

### Dry Run (Default)
Shows what would be restored without making changes:
- Which pipelines would be created/updated
- Which connectors would be loaded
- Any conflicts detected

### Full Restore
```
POST /api/gitops/restore?dry_run=false
```

Reads all `pipelines/*.yaml` and `connectors/*.py` from the GitOps repo and recreates them in the database. Existing pipelines are updated; missing ones are created.

### Point-in-Time Restore

Restore a specific pipeline to a previous version:

```
POST /api/gitops/restore?pipeline_id={id}&commit={sha}
```

Uses git history to find the pipeline YAML at a specific commit and restores that version.

---

## Crash Recovery

On startup, DAPOS detects and handles incomplete state:

### Stale RUNNING Runs
If the process crashed mid-run, `RunRecord` entries with status=RUNNING from before startup are marked as FAILED with error "Process crashed during execution".

### Staging Table Cleanup
Orphaned staging tables from crashed runs are detected and dropped during the first scheduler tick.

### Seed Connector Updates
If `connectors/seeds.py` was modified while the process was down, seed connectors are updated on startup.

---

## Backup Strategies

### YAML Export
```bash
# Full backup of all pipeline configs
python -m cli export > backup_$(date +%Y%m%d).yaml

# Scheduled backup via cron
0 2 * * * cd /opt/dapos && python -m cli export > /backups/pipelines_$(date +\%Y\%m\%d).yaml
```

### Database Backup
Standard PostgreSQL backup tools work:
```bash
pg_dump -h localhost -U dapos dapos > dapos_backup.sql
```

### GitOps Repository
The GitOps repo is a full history of all pipeline changes. Clone it for offline backup:
```bash
git clone /path/to/pipeline-repo /backups/pipeline-repo
```

---

## Recovery Checklist

1. **Database restored?** → Start DAPOS, it auto-migrates tables
2. **Pipelines missing?** → Use GitOps restore (`POST /api/gitops/restore`)
3. **Connectors missing?** → Seeds auto-install; generated connectors restore from GitOps
4. **Runs in bad state?** → Startup auto-cleans stale RUNNING records
5. **Staging tables orphaned?** → Scheduler tick auto-drops orphaned staging
6. **Data contracts lost?** → Re-create from YAML or database backup

---

## Monitoring Recovery

After recovery, verify platform health:

```bash
# Check health
python -m cli health

# Verify all pipelines exist
python -m cli pipelines list

# Check for anomalies
python -m cli anomalies

# Trigger a test run
python -m cli trigger demo-stripe-charges
```
