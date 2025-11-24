# target-vendit

`target-vendit` is a Singer target for Vendit.

Built with the [Meltano Target SDK](https://sdk.meltano.com).

## Installation

```bash
pipx install target-vendit
```

Or install from source:

```bash
pipx install poetry
poetry install
```

## Configuration

### Accepted Config Options

- `api_url` (optional): The base URL for the Vendit API service (default: "https://api2.vendit.online"). Set this to a staging URL if needed.
  - Production: `https://api2.vendit.online`
  - Staging: `https://api.staging.vendit.online`
- `token` (required): Vendit API Token
- `api_key` (required): Vendit API Key

### Example Configuration

**Production:**
```json
{
  "api_url": "https://api2.vendit.online",
  "token": "your-token",
  "api_key": "your-api-key"
}
```

**Staging:**
```json
{
  "api_url": "https://api.staging.vendit.online",
  "token": "your-token",
  "api_key": "your-api-key"
}
```

A full list of supported settings and capabilities for this
target is available by running:

```bash
target-vendit --about
```

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

## Usage

You can easily run `target-vendit` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Target Directly

```bash
target-vendit --version
target-vendit --help
tap-example | target-vendit --config /path/to/target-vendit-config.json
```

## Developer Resources

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `target_vendit/tests` subfolder and then run:

```bash
poetry run pytest
```

You can also test the `target-vendit` CLI interface directly using `poetry run`:

```bash
poetry run target-vendit --help
```

### Testing with [Meltano](https://meltano.com/)

_**Note:** This target will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd target-vendit
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke target-vendit --version
# OR run a test `elt` pipeline:
meltano elt tap-example target-vendit
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano SDK to
develop your own Singer taps and targets.
