# Push logged metrics

Henter loggger produsert av `sfp cli` og pusher de til [sf-github-metrics](https://github.com/navikt/sf-github-metrics) som dytter det videre til Prometheus.

## Usage

<!-- Start usage -->
```yaml
- uses: navikt/sf-gha-push-statsd-metrics@<tag/sha>
    with:
        # Private key for signing messages
        # Required: true
        metricsKey: ''
```
<!-- end usage -->

## Henvendelser

Spørsmål knyttet til koden eller prosjektet kan stilles som issues her på GitHub.

## For NAV-ansatte

Interne henvendelser kan sendes via Slack i kanalen #platforce.
