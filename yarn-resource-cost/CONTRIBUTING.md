# Contributing

Contributions follow the repository-level `CONTRIBUTING.md`, including the
Developer's Certificate of Origin 1.1 and signed-off commits.

Do not contribute customer event logs, cluster logs, bucket names, account
identifiers, internal URLs, or pricing agreements. Tests must use synthetic or
explicitly sanitized fixtures.

Before submitting a change, run:

```bash
python3 -m py_compile yarn-resource-cost/*.py
python3 -m unittest discover -s yarn-resource-cost -p 'test*.py'
python3 yarn-resource-cost/package_yarn_job_cost.py
```
