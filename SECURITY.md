# Security policy

## Supported versions

Calve has no releases yet. Security fixes go to the `main` branch only, so
use the latest `main`, or the GitHub Action at `aiskakov28/calve@main`.

| Version | Supported |
| --- | --- |
| latest `main` | :white_check_mark: |
| older commits | :x: |

## Reporting a vulnerability in Calve

Please don't open a public issue. Report it privately with GitHub's
**Report a vulnerability** button on this repository's Security tab.

What to expect:

- We aim to acknowledge your report within 7 days.
- Once we've confirmed the problem, we'll keep you updated on the fix at least
  every two weeks, and agree on when to disclose it.
- If we decide it isn't a vulnerability, we'll explain why. You're then free to
  raise it as a normal issue.
- With your permission, we'll credit you in the fix.

Calve is a testing tool meant to run against catalogs you control. The
credentials in `docker-compose.yml` and the docs (`root` / `calve-secret`,
`calve` / `calve-password`, the Postgres passwords) are for the local test stack
only. They are not secrets.

## Vulnerabilities in a catalog

If Calve turns up a security problem in a catalog, such as an authorization
bypass, report it to that project's own security contact, not here and not in
a public issue:

- Apache Iceberg, Polaris and Gravitino: [Apache security process](https://www.apache.org/security/)
- Lakekeeper: [lakekeeper/lakekeeper security policy](https://github.com/lakekeeper/lakekeeper/security)
- Nessie: [projectnessie/nessie security policy](https://github.com/projectnessie/nessie/security)
