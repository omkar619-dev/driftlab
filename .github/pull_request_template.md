<!--
Thanks for working on driftlab. Please fill in every section below; the notes
in these comments say when one can be left out. Write it so that someone can
understand the change from this page alone, without reading the commits.
-->

## What this changes

<!-- What is different once this is merged, in two or three sentences. -->

## Why

<!--
The problem this solves, or the plan step it belongs to. Link the issue, the
DESIGN.md section or the upstream report as a full URL. To close an issue on
merge, write "Fixes" and the issue's URL.
A new scenario or driver also quotes the sentence in the client's docs that it
checks (DESIGN.md, First principles, 1).
-->

## How

<!--
The main parts of the change, one bullet each. A pull request built over
several commits lists them as a checklist, ticked as each one lands.
-->

## Contracts

<!--
driftlab keeps three promises to the people who use it. Say for each one
whether this pull request changes it, and how.
-->

- **History format** (DESIGN.md, History format):
- **Driver protocol** (DESIGN.md, Driver split; anything a driver must do differently needs a new protocol number):
- **Verdicts** (can any existing history get a different verdict?):

## Testing

<!--
The commands you ran, what they printed, and the machine they ran on.
A fix or a new rule: show a test failing without the change and passing with it.
A run against a cluster: the scenario, the build and where its history is kept.
-->

## Calibration and results

<!--
Calibrate before you measure (DESIGN.md, First principles, 6). Does a
calibration, or a result in README.md or DESIGN.md, rest on what this changes?
Which runs must be repeated, and have they been? Write "None" if nothing does.
-->

## Not in this pull request

<!-- What is left for later on purpose, so no one takes it for something missed. Leave this section out if there is nothing. -->

## Checklist

<!-- Tick what is done. If an item doesn't apply, say why next to it. -->

- [ ] `gofmt -l` prints nothing and `go vet` is clean, in every module this touches
- [ ] `go test` passes, also with `-race`
- [ ] A test fails without the change, and that output is above
- [ ] DESIGN.md and README.md match what the code now does
- [ ] Every result claimed here or in the docs names the run it comes from
