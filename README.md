# Push logged metrics

Henter loggger produsert av `sfp cli` og pusher de til [sf-github-metrics](https://github.com/navikt/sf-github-metrics) som dytter det videre til Prometheus.

## Usage

<!-- Start usage -->
```yaml
- uses: navikt/sf-gha-push-statsd-metrics@<tag/sha>
    with:
        # Private key for signing messages
        # Required: true
        metrics-key: ''
```
<!-- end usage -->

### Miljøvariabel som kan brukes for å skru av og på pushing av metrics

For å deaktivere pushing av metrics, sett en repository-variabel:

1. Gå til repository **Settings**
2. Naviger til **Secrets and variables** → **Actions**
3. Klikk på **Variables**-fanen
4. Klikk **New repository variable**
5. Opprett variabel:
   - **Name**: `DISABLE_SF_METRICS_PUSH`
   - **Value**: `true`

Når denne variabelen er satt, vil action automatisk hoppe over transformering og pushing av metrics. For å aktivere metrics igjen, slett variabelen eller sett verdien til `false`.

#### Alternative måter å sette variabelen på

**Per workflow:**

```yaml
env:
  DISABLE_SF_METRICS_PUSH: true

jobs:
  my-job:
    runs-on: ubuntu-latest
    steps:
      - uses: navikt/sf-gha-push-statsd-metrics@main
        with:
          metricsKey: ${{ secrets.METRICS_KEY }}
```

**Per job:**

```yaml
jobs:
  my-job:
    runs-on: ubuntu-latest
    env:
      DISABLE_SF_METRICS_PUSH: true
    steps:
      - uses: navikt/sf-gha-push-statsd-metrics@main
        with:
          metricsKey: ${{ secrets.METRICS_KEY }}
```

**Per step:**

```yaml
- uses: navikt/sf-gha-push-statsd-metrics@main
  with:
    metricsKey: ${{ secrets.METRICS_KEY }}
  env:
    DISABLE_SF_METRICS_PUSH: true
```



## Kode generert av GitHub Copilot

Dette repoet bruker GitHub Copilot til å generere kode.

## Henvendelser

Spørsmål knyttet til koden eller prosjektet kan stilles som issues her på GitHub.

## For NAV-ansatte

Interne henvendelser kan sendes via Slack i kanalen #platforce.
