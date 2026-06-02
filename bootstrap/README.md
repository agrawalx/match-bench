# Secret bootstrap

**Why this exists:** the per-service `secret.yaml` manifests ship with *placeholder* values
(`password: ""`, `changeme`). They used to live under `k8s/`, so a routine
`kubectl apply -f k8s/ -R` overwrote the cluster's real credentials with blanks — crashlooping
every service that reads a DB/Kafka/MinIO secret. To stop that, **no secret manifest lives under
`k8s/` anymore.**

## Layout

- `secret-templates/` — the placeholder manifests, moved here for *reference only* (they show each
  secret's namespace + key names). **Committed. Never `kubectl apply` these** — they'd blank out
  real values.
- `secrets-live/` — the **real** secret values, exported from a working cluster, one JSON per
  secret. **Gitignored — never committed.** This is the source of truth for re-bootstrapping.

## Deploy order (fresh cluster)

```sh
# 1. namespaces first
kubectl apply -f k8s/<...namespaces...>

# 2. real secrets (NOT from k8s/, NOT the templates)
kubectl apply -f bootstrap/secrets-live/

# 3. everything else — now safe, k8s/ contains no secrets
kubectl apply -f k8s/ -R
```

If `bootstrap/secrets-live/` is empty (new machine), recreate it from a running cluster with
`kubectl -n <ns> get secret <name> -o json` (strip `metadata.{resourceVersion,uid,creationTimestamp,managedFields}`),
or hand-write the values. Current values set during recovery: DB password `iicpcdbpass`,
MinIO `minioadmin`/`minioadmin123`, Kafka cluster-id `BvFeGlHpO8-2ILZO5bcQMg`, Harbor = placeholders.

## Hard rule

**Never put real secret values in `k8s/`, and never bulk-apply `secret-templates/`.** A stray
`kubectl apply -f k8s/` can no longer touch credentials.

## Next step (out of scope here)

This is the stopgap (option #1). For a git-tracked source of truth, migrate to **Sealed Secrets**
(encrypt-and-commit) or, for EKS, **External Secrets Operator + AWS Secrets Manager** — then
`secrets-live/` goes away.
