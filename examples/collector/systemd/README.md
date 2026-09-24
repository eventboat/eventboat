# systemd unit — install and logrotate notes

`eventboat-collector.service` runs the daemon shape (`run --config-dir`)
under the `eventboat` user, with `/var/lib/eventboat` as the state directory
(SQLite spool + file-source byte offsets) and the pipeline plus Runtime config
under `/etc/eventboat/`.

## logrotate

Prefer **`create` mode** (rename the file, create a fresh one at the same
path). The collector holds the old file's descriptor, so it finishes reading
the rotated file and nothing is lost.

Avoid **`copytruncate`**: the file is rewritten in place, so the collector's
byte offset no longer matches the file — the stream stalls silently (the
file-source v2 work turns that into duplicate reads, design
`docs/design/2026-09-24-log-collection.md` §2.3, but duplicates are still
avoidable). A sample config:

```conf
/var/log/app/*.log {
    daily
    rotate 7
    missingok
    notifempty
    compress
    delaycompress
    create 0644 app app
    sharedscripts
}
```

Keep rotated files around long enough for the collector to read them (the
design's risk table: a file rotated away before it is read is lost) and keep
`rotate`/`maxsize` aligned with the log rate. Reopening the new file at the
same path is hardened by the file-source v2 work (§2.3) — its scenario matrix
(rotation, copytruncate, deleted files) is the acceptance contract — so until
that lands, keep rotation windows generous.
