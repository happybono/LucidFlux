using Microsoft.Office.Interop.Excel;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;
using static System.Windows.Forms.VisualStyles.VisualStyleElement.TaskbarClock;
using Excel = Microsoft.Office.Interop.Excel;

namespace BeamPositionStability
{
    public partial class FrmMain : Form
    {
        // private const int DefaultPointsCapacity = 5000;

        private static readonly DateTime UnixEpochUtc = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private const long MicrosecPerSecond = 1_000_000;
        private const long TicksPerMicrosec = 10; // 1 tick = 100 ns

        private const int ExcelMaxRows = 1_048_576;
        private const int ExcelDataStartRow = 4;

        private int _lastProgressValue;
        private int _progressGeneration;
        private int _activeProgressGeneration;

        private const int ProgressResetDelayMs = 150;

        // private readonly List<PointD> _points = new List<PointD>(5000);
        private readonly List<PointD> _points = new List<PointD>();

        private double[] _xpCache = Array.Empty<double>();
        private double[] _ypCache = Array.Empty<double>();

        private double dpiX;
        private double dpiY;

        private CancellationTokenSource _ctsWork;

        private const string DefaultGuideText =
            "To start, add data (drag and drop a CSV file, paste, or open a file), then choose evaluation settings. Results update automatically.";

        private const string DefaultGuideTextNoData =
            "No data loaded. Add data (drag and drop a CSV file, paste, or open a file). Results update automatically.";

        private void EnsureCacheCapacity(int needed)
        {
            if (_xpCache.Length < needed)
            {
                _xpCache = new double[needed];
                _ypCache = new double[needed];
            }
        }

        public FrmMain()
        {
            InitializeComponent();
        }

        private void FrmMain_Load(object sender, EventArgs e)
        {
            using (Graphics g = this.CreateGraphics())
            {
                dpiX = g.DpiX;
                dpiY = g.DpiY;
            }

            pbMain.Size = new Size(
                (int)(1001 * dpiX / 96),
                (int)(5 * dpiY / 96)
            );

            slblDesc.Size = new Size(
                (int)(1000 * dpiX / 96),
                (int)(19 * dpiY / 96)
            );

            AutoFitColumns();

            if (!rbtnShortTerm.Checked && !rbtnMidTerm.Checked && !rbtnLongTerm.Checked && !rbtnCustomTime.Checked
                && !rbtnAllValues.Checked && !rbtn1000Values.Checked && !rbtnCustomSeq.Checked)
            {
                rbtn1000Values.Checked = true;
            }

            rbtnRad.Checked = true;
            chkOpenBeforeSave.Checked = true;

            txtCustomTime.Enabled = false;
            lblSeconds.Enabled = false;

            lblCustomSeqNumber.Enabled = false;
            lblValues.Enabled = false;

            ToggleTimeCtrls();
            Recalculate();
        }

        #region Shared Progress Infrastructure

        private IProgress<int> CreateUiProgress(int generation)
        {
            return new Progress<int>(p =>
            {
                if (_activeProgressGeneration != generation)
                    return;

                if (p < 0) p = 0;
                if (p > 100) p = 100;

                _lastProgressValue = p;

                pbMain.Style = ProgressBarStyle.Continuous;
                pbMain.Minimum = 0;
                pbMain.Maximum = 100;
                pbMain.Value = p;
            });
        }

        private void ResetProgress()
        {
            pbMain.Style = ProgressBarStyle.Continuous;
            pbMain.Minimum = 0;
            pbMain.Maximum = 100;
            pbMain.Value = 0;
        }

        private void CancelCurrentWork()
        {
            try { _ctsWork?.Cancel(); } catch { /* ignore */ }
            _ctsWork?.Dispose();
            _ctsWork = new CancellationTokenSource();
        }

        private Task<T> RunStaWithProgressAsync<T>(Func<CancellationToken, IProgress<int>, T> work, CancellationToken token, IProgress<int> progress)
        {
            var tcs = new TaskCompletionSource<T>();

            var thread = new Thread(() =>
            {
                try
                {
                    var result = work(token, progress);
                    tcs.TrySetResult(result);
                }
                catch (OperationCanceledException oce)
                {
                    tcs.TrySetCanceled(oce.CancellationToken);
                }
                catch (Exception ex)
                {
                    tcs.TrySetException(ex);
                }
            });

            thread.IsBackground = true;
            thread.SetApartmentState(ApartmentState.STA);
            thread.Start();

            return tcs.Task;
        }

        private async Task RunWithProgressAsync(Func<CancellationToken, IProgress<int>, Task> work, bool resetAfter = true)
        {
            CancelCurrentWork();
            var token = _ctsWork.Token;

            _lastProgressValue = 0;

            int myGen = Interlocked.Increment(ref _progressGeneration);
            _activeProgressGeneration = myGen;

            var progress = CreateUiProgress(myGen);
            progress.Report(0);

            try
            {
                await work(token, progress).ConfigureAwait(true);
            }
            finally
            {
                if (resetAfter)
                {
                    await Task.Yield();

                    pbMain.Refresh();
                    await Task.Delay(ProgressResetDelayMs).ConfigureAwait(true);

                    if (_activeProgressGeneration == myGen)
                        ResetProgress();
                }
            }
        }

        private Task RunStaWithProgressAsync(Action<CancellationToken, IProgress<int>> work, CancellationToken token, IProgress<int> progress)
        {
            var tcs = new TaskCompletionSource<bool>();

            var thread = new Thread(() =>
            {
                try
                {
                    work(token, progress);
                    tcs.TrySetResult(true);
                }
                catch (OperationCanceledException oce)
                {
                    tcs.TrySetCanceled(oce.CancellationToken);
                }
                catch (Exception ex)
                {
                    tcs.TrySetException(ex);
                }
            });

            thread.IsBackground = true;
            thread.SetApartmentState(ApartmentState.STA);
            thread.Start();

            return tcs.Task;
        }

        #endregion

        #region Drag & Drop (Async + Progress)

        private void lvPoints_DragEnter(object sender, DragEventArgs e)
        {
            if (e.Data.GetDataPresent(DataFormats.Text))
            {
                e.Effect = DragDropEffects.Copy;
                return;
            }

            if (e.Data.GetDataPresent(DataFormats.FileDrop))
            {
                var files = e.Data.GetData(DataFormats.FileDrop) as string[];
                if (files != null && files.Length > 0 && files.All(f => string.Equals(Path.GetExtension(f), ".csv", StringComparison.OrdinalIgnoreCase)))
                {
                    e.Effect = DragDropEffects.Copy;
                    return;
                }
            }

            e.Effect = DragDropEffects.None;
        }

        private async void lvPoints_DragDrop(object sender, DragEventArgs e)
        {
            await RunWithProgressAsync(async (token, progress) =>
            {
                try
                {
                    var newPoints = await Task.Run(() =>
                    {
                        token.ThrowIfCancellationRequested();

                        if (e.Data.GetDataPresent(DataFormats.Text))
                        {
                            string text = (string)e.Data.GetData(DataFormats.Text);
                            return ParsePointsFromTextParallelOrdered(text, token, progress);
                        }

                        if (e.Data.GetDataPresent(DataFormats.FileDrop))
                        {
                            var files = (string[])e.Data.GetData(DataFormats.FileDrop);

                            // only accept .csv files.
                            if (!files.All(f => string.Equals(Path.GetExtension(f), ".csv", StringComparison.OrdinalIgnoreCase)))
                                return new List<PointD>();

                            var all = new List<PointD>();
                            for (int i = 0; i < files.Length; i++)
                            {
                                token.ThrowIfCancellationRequested();

                                // scale file progress to 0 .. 30
                                progress.Report(Math.Min(30, (int)Math.Round(30.0 * (i + 1) / files.Length)));

                                string text = File.ReadAllText(files[i]);
                                // parsing progress will do 0 .. 100; map it to 30 .. 95
                                var subProgress = new Progress<int>(p =>
                                    progress.Report(30 + (int)Math.Round(65.0 * p / 100.0)));

                                all.AddRange(ParsePointsFromTextParallelOrdered(text, token, subProgress));
                            }
                            return all;
                        }

                        return new List<PointD>();
                    }, token).ConfigureAwait(true);

                    if (token.IsCancellationRequested)
                        return;

                    // Original behavior: on file drop, if any points loaded, clear existing before add.
                    if (e.Data.GetDataPresent(DataFormats.FileDrop) && newPoints.Count > 0)
                        _points.Clear();

                    AddPoints(newPoints);

                    // Show FitColumns as progress 96 .. 100
                    FitColumns(progress);
                }
                catch (Exception ex)
                {
                    MessageBox.Show(this, ex.Message, "DragDrop Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
                }
            });
        }

        #endregion

        #region Add / Update UI

        private void AddPoints(IEnumerable<PointD> points)
        {
            _points.AddRange(points);

            EnsureCacheCapacity(_points.Count);

            UpdateListView();
            ToggleTimeCtrls();
            Recalculate();
        }

        private void UpdateListView()
        {
            lvPoints.BeginUpdate();
            try
            {
                lvPoints.Items.Clear();

                for (int i = 0; i < _points.Count; i++)
                {
                    var p = _points[i];

                    var item = new ListViewItem((i + 1).ToString());
                    item.SubItems.Add(p.TimestampText ?? string.Empty);
                    item.SubItems.Add(p.X.ToString("G17", CultureInfo.InvariantCulture));
                    item.SubItems.Add(p.Y.ToString("G17", CultureInfo.InvariantCulture));
                    lvPoints.Items.Add(item);
                }
            }
            finally
            {
                lvPoints.EndUpdate();
            }

            UpdateUI();
        }

        private void UpdateUI()
        {
            Text = $"Beam Stability Demo - Points : {_points.Count}";
        }

        #endregion

        #region Parsing (Parallel.For + stable order)

        private static List<PointD> ParsePointsFromTextParallelOrdered(string text, CancellationToken token, IProgress<int> progress)
        {
            if (string.IsNullOrWhiteSpace(text))
                return new List<PointD>();

            var lines = text.Split(new[] { "\r\n", "\n", "\r" }, StringSplitOptions.RemoveEmptyEntries);
            if (lines.Length == 0)
                return new List<PointD>();

            // Identify first header line like the original flow.
            bool csvMode = false;
            char csvDelimiter = ',';
            int tsCol = -1;
            int xCol = -1;
            int yCol = -1;

            int headerIndex = -1;

            for (int i = 0; i < lines.Length; i++)
            {
                token.ThrowIfCancellationRequested();

                string line = lines[i].Trim();
                if (line.Length == 0)
                    continue;

                line = line.TrimStart('\uFEFF');

                if (DetectHeaderLine(line))
                {
                    csvDelimiter = DetectDelimiter(line);
                    var headerCols = SplitDelimitedLine(line, csvDelimiter);

                    tsCol = FindTimestampColumnIndex(headerCols);
                    xCol = FindXColumnIndex(headerCols);
                    yCol = FindYColumnIndex(headerCols);

                    csvMode = true;
                    headerIndex = i;
                    break;
                }
            }

            var perLine = new List<PointD>[lines.Length];

            int processed = 0;
            int reportInterval = Math.Max(1, lines.Length / 100);

            Parallel.For(0, lines.Length, new ParallelOptions
            {
                CancellationToken = token,
                MaxDegreeOfParallelism = Environment.ProcessorCount
            }, i =>
            {
                string rawLine = lines[i];
                if (rawLine == null)
                    return;

                string line = rawLine.Trim();
                if (line.Length == 0)
                    return;

                line = line.TrimStart('\uFEFF');

                // skip header line if found
                if (csvMode && i == headerIndex)
                    return;

                if (csvMode && headerIndex >= 0 && i > headerIndex)
                {
                    var cols = SplitDelimitedLine(line, csvDelimiter);
                    if (TryParsePointFromColumns(cols, tsCol, xCol, yCol, out PointD p))
                        perLine[i] = new List<PointD>(1) { p };
                }
                else
                {
                    char candidateDelimiter = DetectDelimiter(line);
                    if (candidateDelimiter != '\0')
                    {
                        var cols = SplitDelimitedLine(line, candidateDelimiter);
                        if (TryParsePointFromColumns(cols, -1, -1, -1, out PointD p))
                        {
                            perLine[i] = new List<PointD>(1) { p };
                            goto Report;
                        }
                    }

                    if (TryParseXY(line, out double x, out double y))
                        perLine[i] = new List<PointD>(1) { new PointD(null, null, x, y) };
                }

            Report:
                int done = Interlocked.Increment(ref processed);
                if (progress != null && (done % reportInterval) == 0)
                {
                    int pct = (int)Math.Round(100.0 * done / lines.Length);
                    progress.Report(pct);
                }
            });

            var result = new List<PointD>();
            for (int i = 0; i < perLine.Length; i++)
            {
                var list = perLine[i];
                if (list != null && list.Count > 0)
                    result.AddRange(list);
            }

            progress?.Report(100);
            return result;
        }

        private static bool DetectHeaderLine(string line)
        {
            if (line.IndexOf("timestamp", StringComparison.OrdinalIgnoreCase) >= 0)
                return true;

            if (line.IndexOf("centroid", StringComparison.OrdinalIgnoreCase) >= 0)
                return true;

            if (Regex.IsMatch(line, @"\b(x|x\s*\(.*\))\b", RegexOptions.IgnoreCase)
                && Regex.IsMatch(line, @"\b(y|y\s*\(.*\))\b", RegexOptions.IgnoreCase))
                return true;

            return false;
        }

        private static int FindTimestampColumnIndex(IReadOnlyList<string> cols)
        {
            for (int i = 0; i < cols.Count; i++)
            {
                string c = (cols[i] ?? string.Empty).Trim();

                if (c.IndexOf("timestamp", StringComparison.OrdinalIgnoreCase) >= 0)
                    return i;

                if (Regex.IsMatch(c, @"^\s*time(\s*\(.*\))?\s*$", RegexOptions.IgnoreCase))
                    return i;

                if (Regex.IsMatch(c, @"^\s*date(\s*\(.*\))?\s*$", RegexOptions.IgnoreCase))
                    return i;
            }

            return -1;
        }

        private static int FindXColumnIndex(IReadOnlyList<string> cols)
        {
            for (int i = 0; i < cols.Count; i++)
            {
                string c = (cols[i] ?? string.Empty).Trim();

                if (c.IndexOf("centroid", StringComparison.OrdinalIgnoreCase) >= 0
                    && Regex.IsMatch(c, @"\bx\b", RegexOptions.IgnoreCase))
                    return i;

                if (Regex.IsMatch(c, @"^\s*x(\s*\(.*\))?\s*$", RegexOptions.IgnoreCase))
                    return i;
            }

            return -1;
        }

        private static int FindYColumnIndex(IReadOnlyList<string> cols)
        {
            for (int i = 0; i < cols.Count; i++)
            {
                string c = (cols[i] ?? string.Empty).Trim();

                if (c.IndexOf("centroid", StringComparison.OrdinalIgnoreCase) >= 0
                    && Regex.IsMatch(c, @"\by\b", RegexOptions.IgnoreCase))
                    return i;

                if (Regex.IsMatch(c, @"^\s*y(\s*\(.*\))?\s*$", RegexOptions.IgnoreCase))
                    return i;
            }

            return -1;
        }

        private static char DetectDelimiter(string line)
        {
            int comma = CountCharOutsideQuotes(line, ',');
            int semicolon = CountCharOutsideQuotes(line, ';');
            int tab = CountCharOutsideQuotes(line, '\t');

            int max = Math.Max(comma, Math.Max(semicolon, tab));
            if (max <= 0)
                return '\0';

            if (max == tab) return '\t';
            if (max == semicolon) return ';';
            return ',';
        }

        private static int CountCharOutsideQuotes(string s, char ch)
        {
            bool inQuotes = false;
            int count = 0;

            for (int i = 0; i < s.Length; i++)
            {
                char c = s[i];
                if (c == '"')
                {
                    if (inQuotes && i + 1 < s.Length && s[i + 1] == '"')
                    {
                        i++;
                        continue;
                    }

                    inQuotes = !inQuotes;
                    continue;
                }

                if (!inQuotes && c == ch)
                    count++;
            }

            return count;
        }

        private static List<string> SplitDelimitedLine(string line, char delimiter)
        {
            var result = new List<string>();
            if (line == null)
                return result;

            var sb = new StringBuilder();
            bool inQuotes = false;

            for (int i = 0; i < line.Length; i++)
            {
                char c = line[i];

                if (c == '"')
                {
                    if (inQuotes && i + 1 < line.Length && line[i + 1] == '"')
                    {
                        sb.Append('"');
                        i++;
                        continue;
                    }

                    inQuotes = !inQuotes;
                    continue;
                }

                if (!inQuotes && c == delimiter)
                {
                    result.Add(sb.ToString().Trim());
                    sb.Clear();
                    continue;
                }

                sb.Append(c);
            }

            result.Add(sb.ToString().Trim());
            return result;
        }

        #endregion

        #region Parsing helpers (unchanged)

        private static bool TryParsePointFromColumns(IReadOnlyList<string> cols, int tsCol, int xCol, int yCol, out PointD p)
        {
            p = default(PointD);

            if (cols == null || cols.Count < 2)
                return false;

            int xi;
            int yi;

            if (xCol >= 0 && yCol >= 0 && xCol < cols.Count && yCol < cols.Count)
            {
                xi = xCol;
                yi = yCol;
            }
            else
            {
                xi = cols.Count - 2;
                yi = cols.Count - 1;
            }

            if (!TryParseDouble(cols[xi], out double x)) return false;
            if (!TryParseDouble(cols[yi], out double y)) return false;

            int candidateTsCol = -1;
            if (tsCol >= 0 && tsCol < cols.Count)
                candidateTsCol = tsCol;
            else if (cols.Count >= 3)
                candidateTsCol = 0;

            long? tsMicros = null;
            string tsText = null;

            if (candidateTsCol >= 0)
            {
                string rawTs = (cols[candidateTsCol] ?? string.Empty).Trim();
                if (rawTs.Length > 0 && TryParseTimestampToMicrosec(rawTs, out long microseconds))
                {
                    tsMicros = microseconds;
                    tsText = rawTs;
                }
            }

            p = new PointD(timestampMicroseconds: tsMicros, timestampText: tsText, x: x, y: y);
            return true;
        }

        private static bool TryParseXY(string line, out double x, out double y)
        {
            x = y = 0;

            if (string.IsNullOrWhiteSpace(line))
                return false;

            var matches = Regex.Matches(line, @"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?");
            if (matches.Count < 2)
                return false;

            var s1 = matches[matches.Count - 2].Value;
            var s2 = matches[matches.Count - 1].Value;

            if (!TryParseDouble(s1, out x)) return false;
            if (!TryParseDouble(s2, out y)) return false;

            return true;
        }

        private static bool TryParseDouble(string s, out double v)
        {
            var style = NumberStyles.Float | NumberStyles.AllowThousands;

            return double.TryParse(s, style, CultureInfo.CurrentCulture, out v)
                || double.TryParse(s, style, CultureInfo.InvariantCulture, out v);
        }

        private static bool TryParseTimestampToMicrosec(string s, out long microseconds)
        {
            microseconds = 0;

            if (string.IsNullOrWhiteSpace(s))
                return false;

            s = s.Trim();

            if (TryParseTimeSpanToMicrosec(s, out microseconds))
                return true;

            if (TryParseDateTimeToMicrosec(s, out microseconds))
                return true;

            if (TryParseDateTime(s, out DateTime dt))
            {
                DateTime utc = dt.ToUniversalTime();
                long ticks = utc.Ticks - UnixEpochUtc.Ticks;
                microseconds = ticks / TicksPerMicrosec;
                return true;
            }

            return false;
        }

        private static bool TryParseTimeSpanToMicrosec(string s, out long microseconds)
        {
            microseconds = 0;

            if (!Regex.IsMatch(s, @"^\s*\d{1,3}:\d{2}(:\d{2})?([.,]\d+)?\s*$"))
                return false;

            s = s.Replace(',', '.');

            if (s.Count(c => c == ':') == 2)
            {
                string[] parts = s.Split(':');
                if (parts.Length != 3) return false;

                if (!int.TryParse(parts[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out int h)) return false;
                if (!int.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out int m)) return false;

                if (!TryParseFracSecToMicrosec(parts[2], out long secMicros)) return false;

                microseconds = ((long)h * 3600L + (long)m * 60L) * MicrosecPerSecond + secMicros;
                return true;
            }

            if (s.Count(c => c == ':') == 1)
            {
                string[] parts = s.Split(':');
                if (parts.Length != 2) return false;

                if (!int.TryParse(parts[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out int m)) return false;

                if (!TryParseFracSecToMicrosec(parts[1], out long secMicros)) return false;

                microseconds = ((long)m * 60L) * MicrosecPerSecond + secMicros;
                return true;
            }

            return false;
        }

        private static bool TryParseFracSecToMicrosec(string s, out long microseconds)
        {
            microseconds = 0;

            if (string.IsNullOrWhiteSpace(s))
                return false;

            s = s.Trim();

            int dot = s.IndexOf('.');
            if (dot < 0)
            {
                if (!int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out int secWhole))
                    return false;

                microseconds = (long)secWhole * MicrosecPerSecond;
                return true;
            }

            string wholePart = s.Substring(0, dot);
            string fracPart = s.Substring(dot + 1);

            if (!int.TryParse(wholePart, NumberStyles.Integer, CultureInfo.InvariantCulture, out int sec))
                return false;

            if (fracPart.Length > 6)
                fracPart = fracPart.Substring(0, 6);
            fracPart = fracPart.PadRight(6, '0');

            if (!int.TryParse(fracPart, NumberStyles.Integer, CultureInfo.InvariantCulture, out int fracMicros))
                return false;

            microseconds = (long)sec * MicrosecPerSecond + fracMicros;
            return true;
        }

        private static bool TryParseDateTimeToMicrosec(string s, out long microseconds)
        {
            microseconds = 0;

            var m = Regex.Match(
                s,
                @"^\s*(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:[.,](\d{1,7}))?\s*$",
                RegexOptions.CultureInvariant);

            if (!m.Success)
                return false;

            if (!int.TryParse(m.Groups[1].Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int year)) return false;
            if (!int.TryParse(m.Groups[2].Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int month)) return false;
            if (!int.TryParse(m.Groups[3].Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int day)) return false;
            if (!int.TryParse(m.Groups[4].Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int hour)) return false;
            if (!int.TryParse(m.Groups[5].Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int minute)) return false;
            if (!int.TryParse(m.Groups[6].Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int second)) return false;

            string frac = m.Groups[7].Success ? m.Groups[7].Value : string.Empty;

            int micros = 0;
            if (frac.Length > 0)
            {
                string frac7 = frac.Length > 7 ? frac.Substring(0, 7) : frac.PadRight(7, '0');

                if (!int.TryParse(frac7, NumberStyles.Integer, CultureInfo.InvariantCulture, out int ticksFraction))
                    return false;

                micros = (ticksFraction + 5) / 10;
                if (micros >= 1_000_000)
                {
                    micros = 0;
                    second++;
                }
            }

            try
            {
                var dt = new DateTime(year, month, day, hour, minute, 0, DateTimeKind.Unspecified)
                    .AddSeconds(second)
                    .AddTicks(micros * TicksPerMicrosec);

                DateTime utc = dt.ToUniversalTime();
                long ticks = utc.Ticks - UnixEpochUtc.Ticks;
                microseconds = ticks / TicksPerMicrosec;
                return true;
            }
            catch
            {
                return false;
            }
        }

        private static bool TryParseDateTime(string s, out DateTime dt)
        {
            dt = default(DateTime);

            string[] formats =
            {
                "yyyy-MM-dd HH:mm:ss.FFFFFFF",
                "yyyy-MM-dd HH:mm:ss,FFFFFFF",
                "yyyy-MM-dd HH:mm:ss",
                "M/d/yyyy h:mm:ss tt",
                "M/d/yyyy hh:mm:ss tt",
                "M/d/yyyy H:mm:ss",
                "M/d/yyyy HH:mm:ss",
                "M/d/yyyy H:mm:ss.FFFFFFF",
                "M/d/yyyy HH:mm:ss.FFFFFFF",
                "yyyy/MM/dd HH:mm:ss.FFFFFFF",
                "yyyy/MM/dd HH:mm:ss",
            };

            if (DateTime.TryParseExact(s, formats, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out dt))
                return true;

            CultureInfo[] cultures =
            {
                CultureInfo.CurrentCulture,
                CultureInfo.InvariantCulture,
                CultureInfo.GetCultureInfo("en-US"),
                CultureInfo.GetCultureInfo("en-GB"),
                CultureInfo.GetCultureInfo("ko-KR"),
            };

            for (int i = 0; i < cultures.Length; i++)
            {
                if (DateTime.TryParse(s, cultures[i], DateTimeStyles.AllowWhiteSpaces, out dt))
                    return true;
            }

            return false;
        }

        #endregion

        #region Calculation

        private void Recalculate()
        {
            int total = _points.Count;

            GetConsideredRange(total, out int start, out int considered);

            lblTotal.Text = total.ToString();
            lblConsidered.Text = considered.ToString();

            if (considered < 2)
            {
                SetResultColor(isOk: false);
                lblCentroidX.Text = "-";
                lblCentroidY.Text = "-";
                lblAzimuth.Text = "-";
                lblDeltaX.Text = "-";
                lblDeltaY.Text = "-";
                lblDelta.Text = "-";

                return;
            }

            ComputeCentroid(start, total, out double meanX, out double meanY);
            double azimuthRad = ComputeAzimuthFromCov(start, total, meanX, meanY);
            int n = FillRotatedCtrToCache(start, total, meanX, meanY, azimuthRad);

            double stdXp = StdDevSample(_xpCache, n, 0.0);
            double stdYp = StdDevSample(_ypCache, n, 0.0);

            double deltaX = 4.0 * stdXp;
            double deltaY = 4.0 * stdYp;

            double radialStd = Math.Sqrt(stdXp * stdXp + stdYp * stdYp);
            double delta = 2.0 * Math.Sqrt(2.0) * radialStd;

            string azText;
            if (rbtnDeg.Checked)
            {
                double azDeg = azimuthRad * 180.0 / Math.PI;
                azText = azDeg.ToString("F8", CultureInfo.InvariantCulture) + " deg";
            }
            else
            {
                azText = azimuthRad.ToString("F8", CultureInfo.InvariantCulture) + " rad";
            }

            lblCentroidX.Text = $"{meanX.ToString("F6", CultureInfo.InvariantCulture)} mm";
            lblCentroidY.Text = $"{meanY.ToString("F6", CultureInfo.InvariantCulture)} mm";
            lblAzimuth.Text = azText;

            lblDeltaX.Text = $"{deltaX.ToString("F6", CultureInfo.InvariantCulture)} mm";
            lblDeltaY.Text = $"{deltaY.ToString("F6", CultureInfo.InvariantCulture)} mm";
            lblDelta.Text = $"{delta.ToString("F6", CultureInfo.InvariantCulture)} mm";

            SetResultColor(isOk: true);
        }

        private void ComputeCentroid(int start, int total, out double meanX, out double meanY)
        {
            double sx = 0, sy = 0;
            int count = total - start;

            for (int i = start; i < total; i++)
            {
                sx += _points[i].X;
                sy += _points[i].Y;
            }

            meanX = sx / count;
            meanY = sy / count;
        }

        private double ComputeAzimuthFromCov(int start, int total, double meanX, double meanY)
        {
            double sxx = 0, syy = 0, sxy = 0;
            int n = total - start;

            for (int i = start; i < total; i++)
            {
                double dx = _points[i].X - meanX;
                double dy = _points[i].Y - meanY;
                sxx += dx * dx;
                syy += dy * dy;
                sxy += dx * dy;
            }

            double denom = (n - 1);
            sxx /= denom;
            syy /= denom;
            sxy /= denom;

            return 0.5 * Math.Atan2(2.0 * sxy, (sxx - syy));
        }

        private int FillRotatedCtrToCache(int start, int total, double meanX, double meanY, double azimuthRad)
        {
            int count = total - start;

            EnsureCacheCapacity(count);

            double c = Math.Cos(-azimuthRad);
            double s = Math.Sin(-azimuthRad);

            int k = 0;
            for (int i = start; i < total; i++)
            {
                double dx = _points[i].X - meanX;
                double dy = _points[i].Y - meanY;

                double xp = dx * c - dy * s;
                double yp = dx * s + dy * c;

                _xpCache[k] = xp;
                _ypCache[k] = yp;
                k++;
            }

            return count;
        }

        private static double StdDevSample(double[] values, int count, double mean)
        {
            double sumSq = 0;

            for (int i = 0; i < count; i++)
            {
                double d = values[i] - mean;
                sumSq += d * d;
            }

            if (count <= 1) return 0;
            return Math.Sqrt(sumSq / (count - 1));
        }

        #endregion

        #region Considered range / Settings handlers

        private void GetConsideredRange(int total, out int start, out int considered)
        {
            start = 0;
            considered = total;

            if (total <= 0)
                return;

            if (rbtnAllValues.Checked)
            {
                start = 0;
                considered = total;
                return;
            }

            if (rbtn1000Values.Checked)
            {
                considered = Math.Min(total, 1000);
                start = total - considered;
                return;
            }

            if (rbtnCustomSeq.Checked)
            {
                if (TryParsePositiveInt(lblCustomSeqNumber.Text, out int n))
                {
                    considered = Math.Min(total, n);
                    start = total - considered;
                }
                return;
            }

            if (!HasAnyTimestampMicrosec())
            {
                considered = Math.Min(total, 1000);
                start = total - considered;
                return;
            }

            if (!TryGetSelectedWindowMicrosec(out long windowMicros))
            {
                considered = Math.Min(total, 1000);
                start = total - considered;
                return;
            }

            long? lastTs = GetLastTimestampMicrosecOrNull();
            if (lastTs == null)
            {
                considered = Math.Min(total, 1000);
                start = total - considered;
                return;
            }

            long threshold = lastTs.Value - windowMicros;

            int idx = total - 1;
            while (idx >= 0)
            {
                long? ts = _points[idx].TimestampMicroseconds;
                if (ts == null || ts.Value < threshold)
                    break;

                idx--;
            }

            start = Math.Max(0, idx + 1);
            considered = total - start;
        }

        private static bool TryParsePositiveInt(string text, out int value)
        {
            value = 0;

            if (int.TryParse(text, NumberStyles.Integer, CultureInfo.CurrentCulture, out value) && value > 0)
                return true;

            if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out value) && value > 0)
                return true;

            value = 0;
            return false;
        }

        private bool HasAnyTimestampMicrosec()
        {
            for (int i = 0; i < _points.Count; i++)
            {
                if (_points[i].TimestampMicroseconds != null)
                    return true;
            }

            return false;
        }

        private long? GetLastTimestampMicrosecOrNull()
        {
            for (int i = _points.Count - 1; i >= 0; i--)
            {
                var ts = _points[i].TimestampMicroseconds;
                if (ts != null)
                    return ts;
            }

            return null;
        }

        private void ToggleTimeCtrls()
        {
            bool hasAnyTimestamp = HasAnyTimestampMicrosec();

            rbtnShortTerm.Enabled = hasAnyTimestamp;
            rbtnMidTerm.Enabled = hasAnyTimestamp;
            rbtnLongTerm.Enabled = hasAnyTimestamp;
            rbtnCustomTime.Enabled = hasAnyTimestamp;

            bool enableCustomTimeInput = hasAnyTimestamp && rbtnCustomTime.Checked;
            txtCustomTime.Enabled = enableCustomTimeInput;
            lblSeconds.Enabled = enableCustomTimeInput;

            if (!hasAnyTimestamp)
            {
                bool anyTimeChecked = rbtnShortTerm.Checked || rbtnMidTerm.Checked || rbtnLongTerm.Checked || rbtnCustomTime.Checked;
                if (anyTimeChecked)
                    rbtn1000Values.Checked = true;
            }
        }

        private void SetResultColor(bool isOk)
        {
            var c = isOk ? Color.Black : Color.Red;

            lblCentroidX.ForeColor = c;
            lblCentroidY.ForeColor = c;
            lblAzimuth.ForeColor = c;
            lblDeltaX.ForeColor = c;
            lblDeltaY.ForeColor = c;
            lblDelta.ForeColor = c;

            lblTotal.ForeColor = c;
            lblConsidered.ForeColor = c;
        }

        private void rbtnDeg_CheckedChanged(object sender, EventArgs e) => Recalculate();
        private void rbtnRad_CheckedChanged(object sender, EventArgs e) => Recalculate();

        private void SettingsChanged(object sender, EventArgs e)
        {
            ToggleTimeCtrls();
            Recalculate();
        }

        private bool TryGetSelectedWindowMicrosec(out long windowMicroseconds)
        {
            windowMicroseconds = 0;

            if (rbtnShortTerm.Checked)
            {
                windowMicroseconds = 1L * MicrosecPerSecond;
                return true;
            }

            if (rbtnMidTerm.Checked)
            {
                windowMicroseconds = 60L * MicrosecPerSecond;
                return true;
            }

            if (rbtnLongTerm.Checked)
            {
                windowMicroseconds = 3600L * MicrosecPerSecond;
                return true;
            }

            if (rbtnCustomTime.Checked)
            {
                if (!double.TryParse(txtCustomTime.Text, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.CurrentCulture, out double seconds)
                    && !double.TryParse(txtCustomTime.Text, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out seconds))
                {
                    return false;
                }

                if (seconds <= 0)
                    return false;

                windowMicroseconds = (long)Math.Round(seconds * MicrosecPerSecond, MidpointRounding.AwayFromZero);
                return windowMicroseconds > 0;
            }

            return false;
        }

        #endregion

        #region ListView column helpers

        private void FitColumns(IProgress<int> progress)
        {
            // Start column fitting
            progress?.Report(96);

            lvPoints.BeginUpdate();
            try
            {
                // AutoResizeColumns has high performance overhead internally, so progress is incremented midway for better perception
                progress?.Report(98);
                lvPoints.AutoResizeColumns(ColumnHeaderAutoResizeStyle.ColumnContent);
            }
            finally
            {
                lvPoints.EndUpdate();
            }

            // Column fitting completed
            progress?.Report(100);
        }

        private void AutoFitColumns()
        {
            foreach (ColumnHeader col in lvPoints.Columns)
                col.Width = -2;
        }

        #endregion

        #region Clipboard Paste (Async + Progress)

        private async void lvPoints_KeyDown(object sender, KeyEventArgs e)
        {
            if (!(e.Control && e.KeyCode == Keys.V))
                return;

            e.Handled = true;
            e.SuppressKeyPress = true;

            await RunWithProgressAsync(async (token, progress) =>
            {
                try
                {
                    var newPoints = await Task.Run(() =>
                    {
                        token.ThrowIfCancellationRequested();

                        var list = new List<PointD>();

                        if (Clipboard.ContainsText())
                        {
                            string text = Clipboard.GetText();
                            list.AddRange(ParsePointsFromTextParallelOrdered(text, token, progress));
                            return list;
                        }

                        if (Clipboard.ContainsFileDropList())
                        {
                            var files = Clipboard.GetFileDropList();
                            for (int i = 0; i < files.Count; i++)
                            {
                                token.ThrowIfCancellationRequested();

                                // file progress 0 .. 30
                                progress.Report(Math.Min(30, (int)Math.Round(30.0 * (i + 1) / files.Count)));

                                string text = File.ReadAllText(files[i]);
                                var subProgress = new Progress<int>(p =>
                                    progress.Report(30 + (int)Math.Round(65.0 * p / 100.0)));

                                list.AddRange(ParsePointsFromTextParallelOrdered(text, token, subProgress));
                            }
                        }

                        return list;
                    }, token).ConfigureAwait(true);

                    if (newPoints.Count > 0)
                        AddPoints(newPoints);

                    // Display FitColumns in the progress range 96 .. 100
                    FitColumns(progress);
                }
                catch (Exception ex)
                {
                    MessageBox.Show(this, ex.Message, "Paste Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
                }
            });
        }

        #endregion

        #region Open CSV (Async + Progress)

        private async void openCSVFileToolStripMenuItem_Click(object sender, EventArgs e)
        {
            openCSVDialog.Title = "Open CSV";
            openCSVDialog.Filter = "CSV files (*.csv)|*.csv|All files (*.*)|*.*";
            openCSVDialog.FileName = string.Empty;
            openCSVDialog.FilterIndex = 1;
            openCSVDialog.CheckFileExists = true;
            openCSVDialog.CheckPathExists = true;
            openCSVDialog.Multiselect = true;
            openCSVDialog.RestoreDirectory = true;

            if (openCSVDialog.ShowDialog(this) != DialogResult.OK)
                return;

            await RunWithProgressAsync(async (token, progress) =>
            {
                try
                {
                    var files = openCSVDialog.FileNames.ToArray();

                    var newPoints = await Task.Run(() =>
                    {
                        var list = new List<PointD>();
                        for (int i = 0; i < files.Length; i++)
                        {
                            token.ThrowIfCancellationRequested();

                            // file progress 0 .. 30
                            progress.Report(Math.Min(30, (int)Math.Round(30.0 * (i + 1) / files.Length)));

                            string text = File.ReadAllText(files[i]);
                            var subProgress = new Progress<int>(p =>
                                progress.Report(30 + (int)Math.Round(65.0 * p / 100.0)));

                            list.AddRange(ParsePointsFromTextParallelOrdered(text, token, subProgress));
                        }
                        return list;
                    }, token).ConfigureAwait(true);

                    if (newPoints.Count > 0)
                    {
                        _points.Clear();
                        AddPoints(newPoints);
                    }

                    // Display FitColumns in the progress range 96 .. 100
                    FitColumns(progress);
                }
                catch (Exception ex)
                {
                    MessageBox.Show(this, ex.Message, "Open Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
                }
            });
        }

        #endregion

        #region Export Excel (Async + STA + Progress)

        private void exportAsExcelToolStripMenuItem_Click(object sender, EventArgs e)
        {
            btnExport.PerformClick();
        }

        private async void btnExport_Click(object sender, EventArgs e)
        {
            btnExport.Enabled = false;
            btnClear.Enabled = false;

            try
            {
                await RunWithProgressAsync(async (token, progress) =>
                {
                    try
                    {
                        await ExportExcelAsync(token, progress).ConfigureAwait(true);
                    }
                    catch (System.Runtime.InteropServices.COMException ex)
                    {
                        const int REGDB_E_CLASSNOTREG = unchecked((int)0x80040154);
                        if (ex.HResult == REGDB_E_CLASSNOTREG)
                        {
                            var result = MessageBox.Show(
                                this,
                                "Microsoft Excel is not installed (or its COM registration is missing).\n\n" +
                                "Would you like to open the Microsoft Office download page now?",
                                "Excel Not Installed",
                                MessageBoxButtons.YesNo,
                                MessageBoxIcon.Warning,
                                MessageBoxDefaultButton.Button2);

                            if (result == DialogResult.Yes)
                                OpenOfficeDownloadPage();

                            return;
                        }

                        MessageBox.Show(this, "Excel export failed.\n\n" + ex.Message, "Export Error",
                            MessageBoxButtons.OK, MessageBoxIcon.Error);
                    }
                    catch (Exception ex)
                    {
                        MessageBox.Show(this, "An unexpected error occurred.\n\n" + ex.Message, "Export Error",
                            MessageBoxButtons.OK, MessageBoxIcon.Error);
                    }
                }).ConfigureAwait(true);
            }
            finally
            {
                btnExport.Enabled = true;
                btnClear.Enabled = true;
            }
        }

        private sealed class ExportSnapshot
        {
            public string TitleText { get; set; }
            public string DateTimeText { get; set; }
            public bool AllHaveTimestamp { get; set; }

            public int ConsideredStart { get; set; }
            public int ConsideredCount { get; set; }

            public string UnitsText { get; set; }
            public string SelectedSettingText { get; set; }

            public string CentroidXText { get; set; }
            public string CentroidYText { get; set; }
            public string AzimuthText { get; set; }
            public string DeltaXText { get; set; }
            public string DeltaYText { get; set; }
            public string DeltaText { get; set; }

            public string TotalText { get; set; }
            public string ConsideredText { get; set; }

            public bool OpenAfterSave { get; set; }

            public bool ShowExcelAfterCompletedMsg { get; set; }

            public bool ExportConsideredOnly { get; set; }

            public int DataStartIndex { get; set; }
            public int NData { get; set; }

            public string SavePath { get; set; }
        }

        private ExportSnapshot CaptureExportSnapshot()
        {
            var titleText = "Beam Position Stability";
            var dateTimeText = DateTime.Now.ToString("G", CultureInfo.CurrentCulture);
            bool hasAnyTimestampMissing = _points.Any(p => p.TimestampMicroseconds == null || string.IsNullOrWhiteSpace(p.TimestampText));
            bool allHaveTimestamp = !hasAnyTimestampMissing;

            GetConsideredRange(_points.Count, out int start, out int considered);

            bool exportConsideredOnly = chkExportConsideredOnly.Checked;
            int dataStartIndex = exportConsideredOnly ? start : 0;
            int nData = exportConsideredOnly ? considered : _points.Count;

            return new ExportSnapshot
            {
                TitleText = titleText,
                DateTimeText = dateTimeText,
                AllHaveTimestamp = allHaveTimestamp,

                ConsideredStart = start,
                ConsideredCount = considered,

                UnitsText = rbtnDeg.Checked ? "Degrees" : "Radian",
                SelectedSettingText = GetSelectedSettingsText(),

                CentroidXText = lblCentroidX.Text,
                CentroidYText = lblCentroidY.Text,
                AzimuthText = lblAzimuth.Text,
                DeltaXText = lblDeltaX.Text,
                DeltaYText = lblDeltaY.Text,
                DeltaText = lblDelta.Text,

                TotalText = lblTotal.Text,
                ConsideredText = lblConsidered.Text,

                OpenAfterSave = chkOpenBeforeSave.Checked,
                ShowExcelAfterCompletedMsg = chkOpenBeforeSave.Checked,
                ExportConsideredOnly = exportConsideredOnly,

                DataStartIndex = dataStartIndex,
                NData = nData
            };
        }

        private async Task ExportExcelAsync(CancellationToken token, IProgress<int> progress)
        {
            if (_points.Count == 0)
            {
                MessageBox.Show(this, "No points to export.", "Export Excel", MessageBoxButtons.OK, MessageBoxIcon.Information);
                progress.Report(100);
                return;
            }

            var snapshot = CaptureExportSnapshot();

            if (!snapshot.OpenAfterSave)
            {
                using (var sfd = new SaveFileDialog
                {
                    Title = "Save Excel Workbook",
                    Filter = "Excel Workbook (*.xlsx)|*.xlsx",
                    FileName = "Beam Position Stability.xlsx",
                    AddExtension = true,
                    DefaultExt = "xlsx",
                    OverwritePrompt = true
                })
                {
                    if (sfd.ShowDialog(this) != DialogResult.OK)
                        return;

                    snapshot.SavePath = sfd.FileName;
                }
            }

            progress.Report(5);

            await RunStaWithProgressAsync((ct, prog) => ExportExcelCoreSta(snapshot, ct, prog, showExcel: false), token, progress)
                .ConfigureAwait(true);

            progress.Report(100);

            MessageBox.Show(this, "Excel export completed.", "Export Excel",
                MessageBoxButtons.OK, MessageBoxIcon.Information);

            if (snapshot.ShowExcelAfterCompletedMsg)
            {
                await RunStaWithProgressAsync((ct, prog) => ExportExcelCoreSta(snapshot, ct, prog, showExcel: true), token, progress)
                    .ConfigureAwait(true);
            }
        }

        private void ExportExcelCoreSta(ExportSnapshot snapshot, CancellationToken token, IProgress<int> progress, bool showExcel)
        {
            token.ThrowIfCancellationRequested();

            void FinalRelease(object com)
            {
                try
                {
                    if (com != null && System.Runtime.InteropServices.Marshal.IsComObject(com))
                        System.Runtime.InteropServices.Marshal.FinalReleaseComObject(com);
                }
                catch
                {
                    // ignore
                }
            }

            void ReleaseAll(Stack<object> stack)
            {
                while (stack.Count > 0)
                    FinalRelease(stack.Pop());
            }

            int maxRowsPerColumn = ExcelMaxRows - (ExcelDataStartRow - 1);

            // During the showExcel step, the file is only opened in Excel
            if (showExcel)
            {
                if (string.IsNullOrWhiteSpace(snapshot.SavePath))
                    return;

                Excel.Application excelShow = null;
                Excel.Workbooks wbsShow = null;
                Excel.Workbook wbShow = null;

                try
                {
                    excelShow = new Excel.Application();
                    wbsShow = excelShow.Workbooks;
                    wbShow = wbsShow.Open(snapshot.SavePath);

                    excelShow.Visible = true;
                }
                finally
                {
                    // Excel이 열린 상태로 남아야 하므로 Workbook/Workbooks/Application RCW는 해제하지 않음
                    // (해제하면 Excel이 닫히는 케이스가 있어 방치)
                }

                return;
            }

            var coms = new Stack<object>();

            Excel.Application excel = null;
            Excel.Workbooks workbooks = null;
            Excel.Workbook wb = null;
            Excel.Sheets sheets = null;
            Excel.Worksheet ws = null;

            Excel.ChartObjects chartObjects = null;
            Excel.ChartObject chartObj = null;
            Excel.Chart chart = null;
            Excel.SeriesCollection seriesCollection = null;

            try
            {
                progress.Report(10);

                excel = new Excel.Application();
                coms.Push(excel);

                workbooks = excel.Workbooks;
                coms.Push(workbooks);

                wb = workbooks.Add();
                coms.Push(wb);

                sheets = wb.Worksheets;
                coms.Push(sheets);

                ws = (Excel.Worksheet)sheets[1];
                coms.Push(ws);

                ws.Name = snapshot.TitleText;

                ws.Cells[1, 1] = snapshot.TitleText;

                ws.Cells[3, 1] = "Settings";
                ws.Cells[4, 1] = "Units : " + snapshot.UnitsText;
                ws.Cells[5, 1] = "Selection : " + snapshot.SelectedSettingText;

                ws.Cells[7, 1] = "Results";
                ws.Cells[8, 1] = "CentroidX : " + snapshot.CentroidXText;
                ws.Cells[9, 1] = "CentroidY : " + snapshot.CentroidYText;
                ws.Cells[10, 1] = "Azimuth : " + snapshot.AzimuthText;
                ws.Cells[11, 1] = "DeltaX : " + snapshot.DeltaXText;
                ws.Cells[12, 1] = "DeltaY : " + snapshot.DeltaYText;
                ws.Cells[13, 1] = "Delta : " + snapshot.DeltaText;

                ws.Cells[14, 1] = "Count : " + snapshot.TotalText;
                ws.Cells[15, 1] = "Count (Considered) : " + snapshot.ConsideredText;

                ws.Cells[17, 1] = "Exported at : " + snapshot.DateTimeText;

                progress.Report(20);

                int baseCol = 3;

                int indexStartCol = baseCol;
                WriteColumnHeader(ws, indexStartCol, "Index");
                int lastIndexCol = WriteIntColumn(ws, indexStartCol, ExcelDataStartRow, snapshot.NData, maxRowsPerColumn, i => (snapshot.DataStartIndex + i) + 1);

                progress.Report(35);

                int timestampStartCol = lastIndexCol + 2;
                WriteColumnHeader(ws, timestampStartCol, "Timestamp");
                int lastTsCol = WriteStringColumn(ws, timestampStartCol, ExcelDataStartRow, snapshot.NData, maxRowsPerColumn, i => _points[snapshot.DataStartIndex + i].TimestampText ?? string.Empty);

                int xStartCol = lastTsCol + 2;
                WriteColumnHeader(ws, xStartCol, "X");
                int lastXCol = WriteDoubleColumn(ws, xStartCol, ExcelDataStartRow, snapshot.NData, maxRowsPerColumn, i => _points[snapshot.DataStartIndex + i].X);

                int yStartCol = lastXCol + 2;
                WriteColumnHeader(ws, yStartCol, "Y");
                int lastYCol = WriteDoubleColumn(ws, yStartCol, ExcelDataStartRow, snapshot.NData, maxRowsPerColumn, i => _points[snapshot.DataStartIndex + i].Y);

                progress.Report(55);

                // Chart helper columns are ALWAYS based on the considered range
                int nChart = snapshot.ConsideredCount;
                int start = snapshot.ConsideredStart;

                IEnumerable<int> order = Enumerable.Range(start, nChart);
                if (snapshot.AllHaveTimestamp)
                {
                    order = order
                        .OrderBy(i => _points[i].TimestampMicroseconds.Value)
                        .ThenBy(i => i);
                }

                var idxOrdered = new object[nChart, 1];
                var xOrdered = new object[nChart, 1];
                var yOrdered = new object[nChart, 1];

                int k = 0;
                foreach (int i in order)
                {
                    idxOrdered[k, 0] = i + 1; // absolute 1-based index in the original list
                    xOrdered[k, 0] = _points[i].X;
                    yOrdered[k, 0] = _points[i].Y;
                    k++;
                }

                int helperIndexCol = lastYCol + 4;
                WriteColumnHeader(ws, helperIndexCol, snapshot.AllHaveTimestamp ? "Index (ordered by Timestamp) (Considered)" : "Index (ordered by Index) (Considered)");
                int lastHelperIndexCol = WriteObjectColumn(ws, helperIndexCol, ExcelDataStartRow, idxOrdered, nChart, maxRowsPerColumn);

                int helperXCol = lastHelperIndexCol + 2;
                WriteColumnHeader(ws, helperXCol, snapshot.AllHaveTimestamp ? "X (ordered by Timestamp) (Considered)" : "X (ordered by Index) (Considered)");
                int lastHelperXCol = WriteObjectColumn(ws, helperXCol, ExcelDataStartRow, xOrdered, nChart, maxRowsPerColumn);

                int helperYCol = lastHelperXCol + 2;
                WriteColumnHeader(ws, helperYCol, snapshot.AllHaveTimestamp ? "Y (ordered by Timestamp) (Considered)" : "Y (ordered by Index) (Considered)");
                int lastHelperYCol = WriteObjectColumn(ws, helperYCol, ExcelDataStartRow, yOrdered, nChart, maxRowsPerColumn);

                progress.Report(70);

                Excel.Range xUnion = null;
                Excel.Range yUnion = null;

                try
                {
                    xUnion = BuildDataUnionRange(excel, ws, ExcelDataStartRow, nChart, helperXCol, maxRowsPerColumn);
                    yUnion = BuildDataUnionRange(excel, ws, ExcelDataStartRow, nChart, helperYCol, maxRowsPerColumn);

                    int chartColBase = lastHelperYCol + 4;
                    int chartRowBase = 4;

                    chartObjects = (Excel.ChartObjects)ws.ChartObjects();
                    coms.Push(chartObjects);

                    double chartLeft, chartTop;
                    Excel.Range anchorCell = null;
                    try
                    {
                        anchorCell = (Excel.Range)ws.Cells[chartRowBase, chartColBase];
                        chartLeft = anchorCell.Left;
                        chartTop = anchorCell.Top;
                    }
                    finally
                    {
                        FinalRelease(anchorCell);
                    }

                    chartObj = chartObjects.Add(chartLeft, chartTop, 900, 600);
                    coms.Push(chartObj);

                    chart = chartObj.Chart;
                    coms.Push(chart);

                    chart.ChartType = Excel.XlChartType.xlXYScatterLines;
                    chart.HasTitle = true;
                    chart.ChartTitle.Text = snapshot.TitleText;

                    chart.HasLegend = false;

                    chart.Axes(Excel.XlAxisType.xlValue).HasTitle = true;
                    chart.Axes(Excel.XlAxisType.xlValue).AxisTitle.Text = "Y (mm)";

                    chart.Axes(Excel.XlAxisType.xlCategory).HasTitle = true;
                    chart.Axes(Excel.XlAxisType.xlCategory).AxisTitle.Text = "X (mm)";

                    seriesCollection = chart.SeriesCollection();
                    coms.Push(seriesCollection);

                    var series = seriesCollection.NewSeries();
                    try
                    {
                        series.Name = snapshot.AllHaveTimestamp ? "Trajectory (Timestamp order, Considered)" : "Trajectory (Index order, Considered)";
                        series.XValues = xUnion;
                        series.Values = yUnion;
                        series.MarkerStyle = Excel.XlMarkerStyle.xlMarkerStyleCircle;
                        series.MarkerSize = 5;
                    }
                    finally
                    {
                        FinalRelease(series);
                    }
                }
                finally
                {
                    FinalRelease(yUnion);
                    FinalRelease(xUnion);
                }

                progress.Report(85);

                if (string.IsNullOrWhiteSpace(snapshot.SavePath))
                {
                    snapshot.SavePath = Path.Combine(
                        Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments),
                        "Beam Position Stability.xlsx");
                }

                excel.DisplayAlerts = false;
                wb.SaveAs(snapshot.SavePath, Excel.XlFileFormat.xlOpenXMLWorkbook);
                wb.Saved = true;

                progress.Report(100);
            }
            finally
            {
                try { if (excel != null) excel.DisplayAlerts = true; } catch { /* ignore */ }

                try { wb?.Close(false); } catch { /* ignore */ }
                try { excel?.Quit(); } catch { /* ignore */ }

                ReleaseAll(coms);

                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }
        }

        #endregion

        #region Excel helper writers

        private static int WriteObjectColumn(Excel.Worksheet ws, int startCol, int startRow, object[,] values, int count, int maxRowsPerColumn)
        {
            int col = startCol;
            int idx = 0;

            while (idx < count)
            {
                int chunk = Math.Min(maxRowsPerColumn, count - idx);
                var arr = new object[chunk, 1];

                for (int r = 0; r < chunk; r++)
                    arr[r, 0] = values[idx + r, 0];

                Excel.Range top = null, bottom = null, range = null;
                try
                {
                    top = (Excel.Range)ws.Cells[startRow, col];
                    bottom = (Excel.Range)ws.Cells[startRow + chunk - 1, col];
                    range = ws.Range[top, bottom];
                    range.Value2 = arr;
                }
                finally
                {
                    if (range != null) System.Runtime.InteropServices.Marshal.FinalReleaseComObject(range);
                    if (bottom != null) System.Runtime.InteropServices.Marshal.FinalReleaseComObject(bottom);
                    if (top != null) System.Runtime.InteropServices.Marshal.FinalReleaseComObject(top);
                }

                idx += chunk;
                col++;
            }

            return col - 1;
        }

        private static void WriteColumnHeader(Excel.Worksheet ws, int col, string header)
        {
            ws.Cells[3, col] = header;
        }

        private static int WriteIntColumn(Excel.Worksheet ws, int startCol, int startRow, int count, int maxRowsPerColumn, Func<int, int> getValue)
        {
            int col = startCol;
            int idx = 0;

            while (idx < count)
            {
                int chunk = Math.Min(maxRowsPerColumn, count - idx);
                var arr = new object[chunk, 1];

                for (int r = 0; r < chunk; r++)
                    arr[r, 0] = getValue(idx + r);

                Excel.Range top = null, bottom = null, range = null;
                try
                {
                    top = (Excel.Range)ws.Cells[startRow, col];
                    bottom = (Excel.Range)ws.Cells[startRow + chunk - 1, col];
                    range = ws.Range[top, bottom];
                    range.Value2 = arr;
                }
                finally
                {
                    if (range != null) System.Runtime.InteropServices.Marshal.FinalReleaseComObject(range);
                    if (bottom != null) System.Runtime.InteropServices.Marshal.FinalReleaseComObject(bottom);
                    if (top != null) System.Runtime.InteropServices.Marshal.FinalReleaseComObject(top);
                }

                idx += chunk;
                col++;
            }

            return col - 1;
        }

        private static int WriteDoubleColumn(Excel.Worksheet ws, int startCol, int startRow, int count, int maxRowsPerColumn, Func<int, double> getValue)
        {
            int col = startCol;
            int idx = 0;

            while (idx < count)
            {
                int chunk = Math.Min(maxRowsPerColumn, count - idx);
                var arr = new object[chunk, 1];

                for (int r = 0; r < chunk; r++)
                    arr[r, 0] = getValue(idx + r);

                Excel.Range top = null, bottom = null, range = null;
                try
                {
                    top = (Excel.Range)ws.Cells[startRow, col];
                    bottom = (Excel.Range)ws.Cells[startRow + chunk - 1, col];
                    range = ws.Range[top, bottom];
                    range.Value2 = arr;
                }
                finally
                {
                    if (range != null) System.Runtime.InteropServices.Marshal.FinalReleaseComObject(range);
                    if (bottom != null) System.Runtime.InteropServices.Marshal.FinalReleaseComObject(bottom);
                    if (top != null) System.Runtime.InteropServices.Marshal.FinalReleaseComObject(top);
                }

                idx += chunk;
                col++;
            }

            return col - 1;
        }

        private static int WriteStringColumn(Excel.Worksheet ws, int startCol, int startRow, int count, int maxRowsPerColumn, Func<int, string> getValue)
        {
            int col = startCol;
            int idx = 0;

            while (idx < count)
            {
                int chunk = Math.Min(maxRowsPerColumn, count - idx);
                var arr = new object[chunk, 1];

                for (int r = 0; r < chunk; r++)
                    arr[r, 0] = getValue(idx + r) ?? string.Empty;

                Excel.Range top = null, bottom = null, range = null;
                try
                {
                    top = (Excel.Range)ws.Cells[startRow, col];
                    bottom = (Excel.Range)ws.Cells[startRow + chunk - 1, col];
                    range = ws.Range[top, bottom];
                    range.NumberFormat = "@";
                    range.Value2 = arr;
                }
                finally
                {
                    if (range != null) System.Runtime.InteropServices.Marshal.FinalReleaseComObject(range);
                    if (bottom != null) System.Runtime.InteropServices.Marshal.FinalReleaseComObject(bottom);
                    if (top != null) System.Runtime.InteropServices.Marshal.FinalReleaseComObject(top);
                }

                idx += chunk;
                col++;
            }

            return col - 1;
        }

        private static Excel.Range BuildDataUnionRange(Excel.Application excel, Excel.Worksheet ws, int dataStartRow, int totalCount, int colStart, int maxRowsPerColumn)
        {
            int fullCols = totalCount / maxRowsPerColumn;
            int remainder = totalCount - fullCols * maxRowsPerColumn;

            Excel.Range union = null;

            int col = colStart;
            for (int c = 0; c < fullCols + (remainder > 0 ? 1 : 0); c++, col++)
            {
                int rowsInCol = (c < fullCols) ? maxRowsPerColumn : remainder;
                if (rowsInCol <= 0) break;

                Excel.Range top = null, bottom = null, r = null;
                try
                {
                    top = (Excel.Range)ws.Cells[dataStartRow, col];
                    bottom = (Excel.Range)ws.Cells[dataStartRow + rowsInCol - 1, col];
                    r = ws.Range[top, bottom];

                    if (union == null)
                    {
                        union = r;
                        r = null;
                    }
                    else
                    {
                        Excel.Range merged = null;
                        try
                        {
                            merged = excel.Union(union, r);
                        }
                        finally
                        {
                            System.Runtime.InteropServices.Marshal.FinalReleaseComObject(union);
                            System.Runtime.InteropServices.Marshal.FinalReleaseComObject(r);
                        }
                        union = merged;
                        r = null;
                    }
                }
                finally
                {
                    if (r != null) System.Runtime.InteropServices.Marshal.FinalReleaseComObject(r);
                    if (bottom != null) System.Runtime.InteropServices.Marshal.FinalReleaseComObject(bottom);
                    if (top != null) System.Runtime.InteropServices.Marshal.FinalReleaseComObject(top);
                }
            }

            return union;
        }

        #endregion

        #region Text helpers / Clear

        private string GetSelectedSettingsText()
        {
            if (rbtnAllValues.Checked)
                return "All determined values";
            if (rbtn1000Values.Checked)
                return "Last 1000 Values (ISO Standard)";
            if (rbtnCustomSeq.Checked)
                return "Self defined number : " + (lblCustomSeqNumber.Text ?? string.Empty);
            if (rbtnShortTerm.Checked)
                return "Short-term Evaluation (1 sec)";
            if (rbtnMidTerm.Checked)
                return "Mid-term Evaluation (1 min)";
            if (rbtnLongTerm.Checked)
                return "Long-term Evaluation (1 hour)";
            if (rbtnCustomTime.Checked)
                return "Self defined time : " + (txtCustomTime.Text ?? string.Empty) + " second(s)";

            return "Unknown";
        }

        private static void OpenOfficeDownloadPage()
        {
            try
            {
                System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo(
                    "https://www.microsoft.com/microsoft-365/buy/compare-all-microsoft-365-products")
                { UseShellExecute = true });
            }
            catch
            {
                // ignore
            }
        }

        private async void btnClear_Click(object sender, EventArgs e)
        {
            if (_points.Count <= 0)
                return;

            var result = MessageBox.Show(
                this,
                "This will permanently delete all loaded data.\n\nDo you want to continue?",
                "Confirm Clear",
                MessageBoxButtons.YesNo,
                MessageBoxIcon.Warning,
                MessageBoxDefaultButton.Button2);

            if (result != DialogResult.Yes)
                return;

            await RunWithProgressAsync(async (token, progress) =>
            {
                token.ThrowIfCancellationRequested();

                progress.Report(10);

                _points.Clear();
                progress.Report(30);

                Array.Clear(_xpCache, 0, _xpCache.Length);
                Array.Clear(_ypCache, 0, _ypCache.Length);
                progress.Report(50);

                lvPoints.BeginUpdate();
                try
                {
                    lvPoints.Items.Clear();
                    lvPoints.SelectedItems.Clear();
                    progress.Report(80);
                }
                finally
                {
                    lvPoints.EndUpdate();
                }

                ToggleTimeCtrls();
                progress.Report(90);

                Recalculate();
                UpdateUI();

                // Show FitColumns as progress 96 .. 100
                FitColumns(progress);

                await Task.CompletedTask.ConfigureAwait(true);
            });
        }

        #endregion

        #region Data model

        private readonly struct PointD
        {
            public long? TimestampMicroseconds { get; }
            public string TimestampText { get; }
            public double X { get; }
            public double Y { get; }

            public PointD(long? timestampMicroseconds, string timestampText, double x, double y)
            {
                TimestampMicroseconds = timestampMicroseconds;
                TimestampText = timestampText;
                X = x;
                Y = y;
            }
        }

        #endregion

        #region Event Handlers for Settings
        private void rbtnShortTerm_CheckedChanged(object sender, EventArgs e)
        {
            SettingsChanged(sender, e);
        }

        private void rbtnMidTerm_CheckedChanged(object sender, EventArgs e)
        {
            SettingsChanged(sender, e);
        }

        private void rbtnLongTerm_CheckedChanged(object sender, EventArgs e)
        {
            SettingsChanged(sender, e);
        }

        private void rbtnCustomTime_CheckedChanged(object sender, EventArgs e)
        {
            SettingsChanged(sender, e);
        }

        private void txtCustomTime_TextChanged(object sender, EventArgs e)
        {
            SettingsChanged(sender, e);
        }

        private void rbtnAllValues_CheckedChanged(object sender, EventArgs e)
        {
            SettingsChanged(sender, e);
        }

        private void rbtn1000Values_CheckedChanged(object sender, EventArgs e)
        {
            SettingsChanged(sender, e);
        }

        private void rbtnCustomSeq_CheckedChanged(object sender, EventArgs e)
        {
            if (rbtnCustomSeq.Checked)
            {
                lblCustomSeqNumber.Enabled = true;
                lblValues.Enabled = true;
            }
            else
            {
                lblCustomSeqNumber.Enabled = false;
                lblValues.Enabled = false;
            }

            SettingsChanged(sender, e);
        }

        private void lblCustomSeqNumber_TextChanged(object sender, EventArgs e)
        {
            SettingsChanged(sender, e);
        }
        #endregion

        #region Event Handlers for Guide Text
        private void SetDefaultGuideText()
        {
            slblDesc.Text = (_points.Count > 0) ? DefaultGuideText : DefaultGuideTextNoData;
        }

        private void rbtnShortTerm_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Considering all values within the last second of the measurement (short-term stability)";
        }

        private void rbtnShortTerm_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void rbtnMidTerm_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Considering all values within the last minute of the measurement. (medium time span)";
        }

        private void rbtnMidTerm_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void rbtnLongTerm_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Considering all values within the last hour of the measurement (long-term stability).";
        }

        private void rbtnLongTerm_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void rbtnAllValues_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Considering all determined values for the evaluation.";
        }

        private void rbtnAllValues_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void rbtn1000Values_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Considering the last 1000 determined values for the evaluation (as per ISO standard).";
        }

        private void rbtn1000Values_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void rbtnCustomTime_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Considering all values within a user-defined period (activates Time edit control).";
        }

        private void rbtnCustomTime_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void txtCustomTime_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Considering all values within a user-defined period (activates Time edit control).";
        }

        private void txtCustomTime_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void lblSeconds_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Considering all values within a user-defined period activates Time edit control).";
        }

        private void lblSeconds_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void rbtnCustomSeq_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Considering a user-defined number of the most recent values for the evaluation (activates Number label).";
        }

        private void rbtnCustomSeq_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void lblCustomSeqNumber_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Considering a user-defined number of the most recent values for the evaluation (activates Number label).";
        }

        private void lblCustomSeqNumber_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void lblValues_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Considering a user-defined number of the most recent values for the evaluation (activates Number label).";
        }

        private void lblValues_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void chkOpenBeforeSave_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Open the file for editing before saving.";
        }

        private void chkOpenBeforeSave_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void chkExportConsideredOnly_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Export only the data points that were considered in the stability evaluation based on the selected settings.";
        }

        private void chkExportConsideredOnly_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void rbtnDeg_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Azimuth angle displayed in degrees format.";
        }

        private void rbtnDeg_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void rbtnRad_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Azimuth angle displayed in radian format.";
        }

        private void rbtnRad_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void lblTCentroidX_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "X coordinate of the centroid (mean position) of the considered points.";
        }

        private void lblTCentroidX_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void lblCentroidX_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "X coordinate of the centroid (mean position) of the considered points.";
        }

        private void lblCentroidX_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void lblTCentroidY_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Y coordinate of the centroid (mean position) of the considered points.";
        }

        private void lblTCentroidY_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void lblCentroidY_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Y coordinate of the centroid (mean position) of the considered points.";
        }

        private void lblCentroidY_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void lblTAzimuth_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Azimuth between camera coordinate system and the direction of maximal deflection of the asymmetric beam axes allocation in the far field. (see ISO-11670)";
        }

        private void lblTAzimuth_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void lblAzimuth_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Azimuth between camera coordinate system and the direction of maximal deflection of the asymmetric beam axes allocation in the far field. (see ISO-11670)";
        }

        private void lblAzimuth_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void lblTDeltaX_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Horizontal (x) beam position stability. Standard deviation of the X coordinates of the considered points.";
        }

        private void lblTDeltaX_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void lblDeltaX_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Horizontal (x) beam position stability. Standard deviation of the X coordinates of the considered points.";
        }

        private void lblDeltaX_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void lblTDeltaY_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Vertical (y) beam position stability. Standard deviation of the Y coordinates of the considered points.";
        }

        private void lblTDeltaY_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void lblDeltaY_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Vertical (y) beam position stability. Standard deviation of the Y coordinates of the considered points.";
        }

        private void lblDeltaY_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void lblTDelta_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Beam position stability at rotational symmetry. ( Δx / Δy ⩽ 1.15 )";
        }

        private void lblTDelta_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void lblDelta_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Beam position stability at rotational symmetry. ( Δx / Δy ⩽ 1.15 )";
        }

        private void lblDelta_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void lblTCount_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Number of all centroid values in the current measurement sample.";
        }

        private void lblTCount_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void lblTotal_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Number of all centroid values in the current measurement sample."; 
        }

        private void lblTotal_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void lblTConsidered_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Number of centroid values considered for the stability evaluation based on the selected settings.";
        }

        private void lblTConsidered_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void lblConsidered_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Number of centroid values considered for the stability evaluation based on the selected settings.";
        }

        private void lblConsidered_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void btnExport_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Export the measurement data and evaluation results to an Excel file.";
        }

        private void btnExport_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void btnClear_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Clear all loaded measurement data.";
        }

        private void btnClear_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void lvPoints_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Add data here : drag and drop CSV files, or paste text / CSV from the clipboard (Ctrl + V). Values are parsed and results update automatically.";
        }

        private void lvPoints_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void openCSVToolStripMenuItem_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Open a CSV file containing beam position data.";
        }

        private void openCSVToolStripMenuItem_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }

        private void exportAsExcelToolStripMenuItem_MouseHover(object sender, EventArgs e)
        {
            slblDesc.Text = "Export the measurement data and evaluation results to an Excel file.";
        }

        private void exportAsExcelToolStripMenuItem_MouseLeave(object sender, EventArgs e)
        {
            SetDefaultGuideText();
        }
        #endregion
    }
}


