# LucidFlux
**LucidFlux** is a Windows Forms utility to evaluate **beam position stability** from a sequence of measured centroid points `(X, Y)` (optionally with timestamps).
It supports drag-and-drop / paste of text or files containing point data, then computes centroid, azimuth (principal axis), and stability metrics.

## Features
- Drag & drop point data onto the list
- Paste point data via `Ctrl + V`
- Accepts plain numeric text or delimited text (CSV / TSV / semicolon)
- Optional timestamp parsing enables time-window evaluation modes
- Displays:
  - Centroid `(meanX, meanY)`
  - Azimuth (principal axis angle)
  - `ΔX`, `ΔY`, and `Δ`

<br>

<div align="center">
<img alt="GitHub Last Commit" src="https://img.shields.io/github/last-commit/happybono/LucidFlux"> 
<img alt="GitHub Repo Size" src="https://img.shields.io/github/repo-size/happybono/LucidFlux">
<img alt="GitHub Repo Languages" src="https://img.shields.io/github/languages/count/happybono/LucidFlux">
<img alt="GitHub Top Languages" src="https://img.shields.io/github/languages/top/happybono/LucidFlux">
</div>

<br>

## What's New
### v1.0.0.0
#### January 04, 2026
> Initial release.

## Required Components & Setup
- Target framework: **.NET Framework 4.8.1**
- Language version: **C# 7.3**

### Initial Setup
- Clone or download the repository.
- Open the solution file (.slnx) in Visual Studio.
- Add necessary references if required.
- Build the project.
- Run the application.
- Add data by drag-and-drop or paste into the list.

## Input formats
Data can be provided as:
- Plain text lines containing at least two numbers (the last two numeric tokens are treated as `X` and `Y`).
- Delimited text with optional header (delimiter auto-detected among `,`, `;`, tab).

### Header / column detection
If a header line is detected, the application attempts to locate:
- Timestamp column: `timestamp`, `time`, `date`
- X column: `x`, `centroid x`
- Y column: `y`, `centroid y`

If columns cannot be detected, the last two columns are treated as `X` and `Y`.

### Timestamp parsing
Timestamp values are optional. When present, the application attempts to parse:

- Time-span format : `mm:ss(.fff...)` or `hh:mm:ss(.fff...)`
- Date-time format : `yyyy-MM-dd HH:mm:ss(.fffffff)` or ISO-like `yyyy-MM-ddTHH:mm:ss(.fffffff)`
- Other common locale formats (best-effort)

Parsed timestamps are converted to microseconds relative to the Unix epoch.

## Evaluation window (Settings)
The application supports two selection strategies:

### Value-based
- **All determined values**
- **Last 1000 values (ISO Standard)**
- **Self-defined number**

### Time-based (enabled only when timestamps exist)
- Short-term Evaluation (1 sec)
- Mid-term Evaluation (1 min)
- Long-term Evaluation (1 hour)
- Self-defined time (seconds)

If timestamps are missing, time-based controls are disabled, and the application falls back to a value-based mode (up to the last 1000 points).

## Computation details
Given the selected subset of points:

1. **Centroid**
   - `meanX = average(X)`
   - `meanY = average(Y)`

2. **Azimuth (principal axis)**
   - Computed from the covariance terms:
     - `azimuthRad = 0.5 * atan2(2 * sxy, (sxx - syy))`

3. **Rotation**
   - Center the points on the centroid and rotate by `-azimuthRad` producing `(x', y')`.

4. **Sample standard deviation**
   - Uses **sample** standard deviation (`n - 1`).

5. **Reported metrics**
   - `ΔX = 4 * σx'`
   - `ΔY = 4 * σy'`
   - `Δ  = 2 * sqrt(2) * sqrt(σx'^2 + σy'^2)`

Angle units can be displayed in radians or degrees.

## Limits
- Max stored points : **5000**

## References
> The UI label "Last 1000 values (ISO Standard)" suggests alignment with an ISO evaluation convention.

- **ISO 11146 (series)** : Lasers and laser-related equipment  
  Test methods for laser beam widths, divergence angles and beam propagation ratios.

- **ISO 11147 (series)** : Lasers and laser-related equipment  
  Test methods for laser beam parameters.
