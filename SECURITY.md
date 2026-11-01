# Security policy

## Reporting a vulnerability in Calve

Please don't open a public issue. Report it privately with GitHub's
**Report a vulnerability** button on this repository's Security tab. We'll
reply as soon as we can and agree on a fix and disclosure with you.

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
