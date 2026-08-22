# YARN resource-proportional worker cost model

For every completed executor container `c` on node class `h`:

```text
DurationSeconds(c) = (authoritative RM finish - RM allocation) / 1000
```

With `DefaultResourceCalculator`:

```text
Share(c, h) = AllocatedMemoryMB(c) / AdvertisedMemoryMB(h)
```

With `DominantResourceCalculator`:

```text
Share(c, h) = max over allocated resources r of
              Allocated(c, r) / Advertised(h, r)
```

The unpriced result retains every heterogeneous node class:

```text
NodeClassSeconds(h) = sum(DurationSeconds(c) * Share(c, h))
```

Given an auditable hourly worker rate:

```text
WorkerCost = sum(NodeClassSeconds(h) * HourlyRate(h) / 3600)
```

ApplicationMaster containers are excluded by default. Wall-clock duration,
successful-task duration, perfect-packing estimates, Spark executor cores, and
raw YARN vcore-seconds are diagnostics only. They never replace the allocation
ledger or determine the node share.

An application is complete only when the selected Spark log is complete and
the RM evidence accounts for every allocated executor container, its terminal
time, its node, all allocated resource capacities, and its stable node class.
An incomplete application retains resource evidence and warnings but has no
final price or price factor.
