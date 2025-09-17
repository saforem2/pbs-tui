# PBS Pro Textual TUI


<center>
<img width="48%" alt="ScreenShot-2025-09-16-172415@2x" src="https://github.com/user-attachments/assets/0947b9f3-3b55-42b9-8a6d-8e301492b7f7" /> <img width="48%" alt="ScreenShot-2025-09-16-172702@2x" src="https://github.com/user-attachments/assets/c3a5a8c4-3e28-4ec3-b4e4-91438f745ced" />
</center>

A terminal user interface built with [Textual](https://textual.textualize.io/) for monitoring
[PBS Pro](https://altair.com/pbs-professional) schedulers at the
[Argonne Leadership Computing Facility](https://alcf.anl.gov). The dashboard surfaces job,
queue, and node activity in a single view and refreshes itself automatically so operators can
track workload health in real time.

- Try it out!

  ```bash
  uv run --with pbs-tui pbs-tui
  ```

## Features

- **Live PBS data** – prefers the JSON (`-F json`) output of `qstat`/`pbsnodes` and falls back to
  XML or text parsing so schedulers without newer flags continue to work.
- **Automatic refresh** – updates every 30 seconds by default with a manual refresh binding
  (`r`).
- **Summary cards** – quick totals for job states, node states, and queue health.
- **Rich tables** – sortable (via cursor) tables for jobs, nodes, and queues with detail views
  for the selected record.
- **Fallback sample data** – optional bundled data makes it easy to demo the interface without
  connecting to a production scheduler (`PBS_TUI_SAMPLE_DATA=1`).
- **Inline snapshot** – render the current queue as a Rich table with `pbs-tui --inline` and
  optionally write a Markdown summary alongside it.

## Installation

```bash
uv pip install pbs-tui
```

<!--
1. Ensure Python 3.10 or newer is available.
2. Install the project (and Textual) in your environment:

   ```bash
   pip install -e .
   ```

   Development extras (formatting, etc.) can be installed with `pip install -e .[dev]`.
-->

## Usage

Launch the dashboard once the PBS CLI utilities (`qstat`, `pbsnodes`) are on the `PATH`:

```bash
pbs-tui
```

The same entry point is available via `python -m pbs_tui`. The interface displays a summary
panel, tables for jobs/nodes/queues, and a detail pane for the selected row. Refreshing happens
automatically; press `r` to force an immediate update.

Adjust the refresh cadence with `pbs-tui --refresh-interval 60` (seconds) if you prefer a slower or
faster polling loop.

### Key bindings

| Key |       Action           |
|:---:|:---------------------- |
| `q` | Quit the application   |
| `r` | Refresh immediately    |
| `j` | Focus the jobs table   |
| `n` | Focus the nodes table  |
| `u` | Focus the queues table |

Use the arrow keys/`PageUp`/`PageDown` to move through rows once a table has focus.

### Sample mode

If you want to explore the UI without a live PBS cluster, export `PBS_TUI_SAMPLE_DATA=1`
(or pass `force_sample=True` to `PBSDataFetcher`). The application will display bundled example
jobs, nodes, and queues along with a warning banner indicating that the data is synthetic.

### Headless / automated runs

For automated testing or CI environments without an interactive terminal you can run the TUI in
headless mode by exporting `PBS_TUI_HEADLESS=1`. Pairing this with `PBS_TUI_AUTOPILOT=quit`
presses the `q` binding automatically after startup so `pbs-tui` exits cleanly once the interface
has rendered its first update.

### Inline snapshot mode

When running non-interactively you can emit a Rich-rendered table summarising the active PBS jobs
instead of starting the Textual interface:

```bash
PBS_TUI_SAMPLE_DATA=1 pbs-tui --inline
```

The command prints a table that can be pasted into terminals that support Unicode box drawing. Pass
`--file snapshot.md` alongside `--inline` to also write an aligned Markdown table to `snapshot.md`
for sharing in chat or documentation systems. Any warnings raised while collecting data are written
to standard error so they remain visible in logs.

## Architecture

- `pbs_tui.fetcher.PBSDataFetcher` orchestrates `qstat`/`pbsnodes` calls, preferring JSON output and
  falling back to XML/text before converting everything into structured dataclasses (`Job`, `Node`,
  `Queue`).
- `pbs_tui.app.PBSTUI` is the Textual application that renders the dashboard, periodically asks
  the fetcher for new data, and updates the widgets.
- `pbs_tui.samples.sample_snapshot` provides the demonstration snapshot used when PBS commands
  cannot be executed.

The UI styles are defined in `pbs_tui/app.tcss`. Adjust the CSS to change layout or theme
attributes.

## Development notes

- The application refresh interval defaults to 30 seconds. Pass a different value to
  `PBSTUI(refresh_interval=...)` if desired.
- Errors encountered while running PBS commands are surfaced in the status bar so operators can
  quickly see when data is stale.
- When both PBS utilities are unavailable and the fallback is disabled, the UI will show an empty
  dashboard with an error message in the status bar.

## Screenshots

- `pbs-tui`:

  <img width="2498" height="1828" alt="ScreenShot-2025-09-16-172415@2x" src="https://github.com/user-attachments/assets/419cecb6-25a1-4007-8456-38bd80fb4ae7" />


- Keys and Help Panel:

  <img width="2498" height="1828" alt="ScreenShot-2025-09-16-172451@2x" src="https://github.com/user-attachments/assets/d521d137-1135-4503-bcc0-2b9dba35d252" />
  
- Command palette:
  
  <img width="2498" height="1828" alt="ScreenShot-2025-09-16-172546@2x" src="https://github.com/user-attachments/assets/5804c99a-621a-4cce-adde-092f6d324824" />

- theme support:
  
  <img width="2498" height="1828" alt="ScreenShot-2025-09-16-172702@2x" src="https://github.com/user-attachments/assets/d4009439-2ea7-49f5-9c75-5d25f7b13771" />
