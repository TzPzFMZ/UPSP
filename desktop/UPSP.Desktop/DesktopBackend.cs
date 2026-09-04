using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace UPSP.Desktop;

internal sealed record RuntimeSnapshot(
    bool Connected,
    bool HasActiveOperation,
    bool CanStop,
    int? Round,
    string Stage,
    string Outcome,
    string Settlement,
    bool StopOutcomeSafe,
    bool RestartRequested)
{
    internal string DisplayText =>
        !Connected ? "后端已停止"
        : HasActiveOperation
            ? Round is int round ? $"轮次 {round} · {Stage}" : $"运行中 · {Stage}"
            : "空闲";
}

internal sealed class DesktopBackend : IDisposable
{
    private const string ControlHeader = "X-UPSP-Desktop-Control";
    private readonly string _programRoot;
    private readonly string _token = Hex(32);
    private readonly string _sessionId = Hex(16);
    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(5) };
    private readonly ConcurrentQueue<string> _stderr = new();
    private Process? _process;
    private JobObject? _job;

    internal DesktopBackend(string programRoot)
    {
        _programRoot = Path.GetFullPath(programRoot);
    }

    internal Uri Origin { get; private set; } = new("http://127.0.0.1:8770/");
    internal bool HasExited => _process?.HasExited != false;

    internal async Task StartAsync()
    {
        var python = Path.Combine(_programRoot, "runtime", "python", "python.exe");
        var server = Path.Combine(_programRoot, "tools", "serve_seed_gui.py");
        if (!File.Exists(python) || !File.Exists(server))
        {
            throw new InvalidOperationException("安装文件不完整，请重新安装 UPSP。");
        }

        var localRoot = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "UPSP");
        var cacheRoot = Path.Combine(localRoot, "cache", "python");
        Directory.CreateDirectory(cacheRoot);
        Directory.CreateDirectory(Path.Combine(localRoot, "logs"));

        var start = new ProcessStartInfo
        {
            FileName = python,
            WorkingDirectory = _programRoot,
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        start.ArgumentList.Add("-I");
        start.ArgumentList.Add("-X");
        start.ArgumentList.Add("utf8");
        start.ArgumentList.Add("-X");
        start.ArgumentList.Add($"pycache_prefix={cacheRoot}");
        start.ArgumentList.Add(server);
        start.ArgumentList.Add("--desktop");
        start.ArgumentList.Add("--port");
        start.ArgumentList.Add("8770");
        start.Environment["UPSP_DESKTOP_CONTROL_TOKEN"] = _token;
        start.Environment["UPSP_DESKTOP_SESSION_ID"] = _sessionId;
        start.Environment["PYTHONDONTWRITEBYTECODE"] = "1";

        _process = Process.Start(start)
            ?? throw new InvalidOperationException("无法启动 UPSP 本地后端。");
        try
        {
            _job = new JobObject();
            _job.Add(_process);
        }
        catch
        {
            TerminateStartedProcess();
            _job?.Dispose();
            _job = null;
            throw;
        }
        _ = DrainErrorsAsync(_process);

        string? readyLine;
        try
        {
            readyLine = await _process.StandardOutput.ReadLineAsync()
                .WaitAsync(TimeSpan.FromSeconds(30));
        }
        catch (TimeoutException)
        {
            throw StartupFailure("本地后端启动超时。");
        }
        if (string.IsNullOrWhiteSpace(readyLine))
        {
            throw StartupFailure("本地后端未返回启动记录。");
        }

        using var ready = JsonDocument.Parse(readyLine);
        var root = ready.RootElement;
        var schema = Text(root, "schema_version");
        var session = Text(root, "session_id");
        var origin = Text(root, "origin");
        var productVersion = Text(root, "product_version");
        var processId = Number(root, "process_id");
        var expectedVersion = typeof(DesktopBackend).Assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
            .InformationalVersion ?? "";
        if (schema != "upsp_desktop_ready.v1"
            || session != _sessionId
            || processId != _process.Id
            || origin != "http://127.0.0.1:8770"
            || productVersion != expectedVersion)
        {
            throw StartupFailure("本地后端启动记录校验失败。");
        }
        Origin = new Uri(origin + "/", UriKind.Absolute);
        _ = DrainOutputAsync(_process);
    }

    internal async Task<RuntimeSnapshot> GetStatusAsync()
    {
        if (HasExited)
        {
            return new(false, false, false, null, "", "", "", false, false);
        }
        using var response = await _http.GetAsync(new Uri(Origin, "api/runtime/status"));
        response.EnsureSuccessStatusCode();
        using var document = JsonDocument.Parse(await response.Content.ReadAsStreamAsync());
        var root = document.RootElement;
        if (Text(root, "schema_version") != "seed_gui_runtime_status.v3")
        {
            throw new InvalidOperationException("本地后端状态版本不受支持。");
        }
        var round = OptionalNumber(root, "current_round");
        var active = Boolean(root, "send_in_flight")
            || Boolean(root, "relay_in_flight")
            || Boolean(root, "mutation_in_flight")
            || round.HasValue;
        var outcome = "";
        var settlement = "";
        var stopSafe = false;
        if (root.TryGetProperty("last_outcome", out var last)
            && last.ValueKind == JsonValueKind.Object)
        {
            outcome = Text(last, "status");
            settlement = Text(last, "settlement_status");
            var stoppedWithoutRound = outcome == "round_stopped"
                && OptionalNumber(last, "round_num") is null;
            stopSafe = outcome == "round_stopped"
                && (stoppedWithoutRound || settlement is "degraded" or "settled" or "closed");
        }
        return new(
            true,
            active,
            Boolean(root, "can_stop"),
            round,
            Text(root, "stage"),
            outcome,
            settlement,
            stopSafe,
            Boolean(root, "restart_requested"));
    }

    internal async Task<bool> RequestStopAsync()
    {
        using var response = await PostAsync("api/runtime/stop", includeControlToken: false);
        if (response.StatusCode == HttpStatusCode.Conflict)
        {
            using var payload = JsonDocument.Parse(
                await response.Content.ReadAsStreamAsync());
            if (Text(payload.RootElement, "error") == "no_round_in_flight")
            {
                return false;
            }
        }
        response.EnsureSuccessStatusCode();
        using var receipt = JsonDocument.Parse(
            await response.Content.ReadAsStreamAsync());
        return Text(receipt.RootElement, "reason") != "local_cleanup_in_progress";
    }

    internal async Task<RuntimeSnapshot> WaitForIdleAsync(TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        RuntimeSnapshot snapshot;
        do
        {
            await Task.Delay(250);
            snapshot = await GetStatusAsync();
            if (!snapshot.HasActiveOperation)
            {
                return snapshot;
            }
        }
        while (DateTime.UtcNow < deadline);
        return snapshot;
    }

    internal async Task<bool> ShutdownAsync()
    {
        if (HasExited)
        {
            return true;
        }
        using var response = await PostAsync("api/desktop/shutdown", includeControlToken: true);
        if (response.StatusCode != HttpStatusCode.Accepted)
        {
            return false;
        }
        try
        {
            await _process!.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(15));
            return true;
        }
        catch (TimeoutException)
        {
            DesktopLog.Write(
                "Graceful backend shutdown timed out; terminating the managed process tree.");
            TerminateStartedProcess();
            return HasExited;
        }
    }

    internal async Task RestartAsync()
    {
        if (!await ShutdownAsync())
        {
            throw new InvalidOperationException("本地后端未能安全关闭，无法切换位格或分身。");
        }
        _process?.Dispose();
        _process = null;
        _job?.Dispose();
        _job = null;
        while (_stderr.TryDequeue(out _))
        {
        }
        await StartAsync();
    }

    private async Task<HttpResponseMessage> PostAsync(
        string relativePath,
        bool includeControlToken)
    {
        using var request = new HttpRequestMessage(
            HttpMethod.Post,
            new Uri(Origin, relativePath));
        request.Headers.TryAddWithoutValidation("Origin", Origin.GetLeftPart(UriPartial.Authority));
        if (includeControlToken)
        {
            request.Headers.TryAddWithoutValidation(ControlHeader, _token);
        }
        request.Content = new StringContent("{}", Encoding.UTF8, "application/json");
        return await _http.SendAsync(request);
    }

    private async Task DrainErrorsAsync(Process process)
    {
        while (await process.StandardError.ReadLineAsync() is { } line)
        {
            _stderr.Enqueue(line);
            while (_stderr.Count > 30)
            {
                _stderr.TryDequeue(out _);
            }
            DesktopLog.Write(line);
        }
    }

    private static async Task DrainOutputAsync(Process process)
    {
        while (await process.StandardOutput.ReadLineAsync() is not null)
        {
        }
    }

    private void TerminateStartedProcess()
    {
        try
        {
            if (_process?.HasExited == false)
            {
                _process.Kill(entireProcessTree: true);
                _process.WaitForExit(5000);
            }
        }
        catch
        {
            // Best effort after the process failed to enter the Job Object.
        }
    }

    private Exception StartupFailure(string message)
    {
        var detail = string.Join(Environment.NewLine, _stderr.TakeLast(8));
        return new InvalidOperationException(
            string.IsNullOrWhiteSpace(detail) ? message : $"{message}\n{detail}");
    }

    private static string Text(JsonElement root, string name) =>
        root.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString() ?? ""
            : "";

    private static bool Boolean(JsonElement root, string name) =>
        root.TryGetProperty(name, out var value)
        && value.ValueKind is JsonValueKind.True or JsonValueKind.False
        && value.GetBoolean();

    private static int Number(JsonElement root, string name) =>
        root.TryGetProperty(name, out var value)
        && value.ValueKind == JsonValueKind.Number
        && value.TryGetInt32(out var number)
            ? number
            : -1;

    private static int? OptionalNumber(JsonElement root, string name) =>
        root.TryGetProperty(name, out var value)
        && value.ValueKind == JsonValueKind.Number
        && value.TryGetInt32(out var number)
            ? number
            : null;

    private static string Hex(int bytes) =>
        Convert.ToHexString(RandomNumberGenerator.GetBytes(bytes)).ToLowerInvariant();

    public void Dispose()
    {
        _http.Dispose();
        _process?.Dispose();
        _job?.Dispose();
    }
}

internal static class DesktopLog
{
    private static readonly object Sync = new();

    internal static void Write(Exception exc) => Write(exc.ToString());

    internal static void Write(string text)
    {
        try
        {
            var root = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                "UPSP",
                "logs");
            Directory.CreateDirectory(root);
            lock (Sync)
            {
                File.AppendAllText(
                    Path.Combine(root, "desktop.log"),
                    $"{DateTimeOffset.Now:O} {text}{Environment.NewLine}",
                    Encoding.UTF8);
            }
        }
        catch
        {
            // Logging must never stop cleanup or shutdown.
        }
    }
}

internal sealed class JobObject : IDisposable
{
    private const uint ExtendedLimitInformationClass = 9;
    private const uint JobObjectLimitKillOnJobClose = 0x00002000;
    private IntPtr _handle;

    internal JobObject()
    {
        _handle = CreateJobObject(IntPtr.Zero, null);
        if (_handle == IntPtr.Zero)
        {
            throw new System.ComponentModel.Win32Exception();
        }
        var information = new JobObjectExtendedLimitInformation
        {
            BasicLimitInformation = new JobObjectBasicLimitInformation
            {
                LimitFlags = JobObjectLimitKillOnJobClose,
            },
        };
        var length = Marshal.SizeOf(information);
        var pointer = Marshal.AllocHGlobal(length);
        try
        {
            Marshal.StructureToPtr(information, pointer, false);
            if (!SetInformationJobObject(
                _handle,
                ExtendedLimitInformationClass,
                pointer,
                (uint)length))
            {
                throw new System.ComponentModel.Win32Exception();
            }
        }
        finally
        {
            Marshal.FreeHGlobal(pointer);
        }
    }

    internal void Add(Process process)
    {
        if (!AssignProcessToJobObject(_handle, process.Handle))
        {
            throw new System.ComponentModel.Win32Exception();
        }
    }

    public void Dispose()
    {
        if (_handle != IntPtr.Zero)
        {
            CloseHandle(_handle);
            _handle = IntPtr.Zero;
        }
    }

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode)]
    private static extern IntPtr CreateJobObject(IntPtr securityAttributes, string? name);

    [DllImport("kernel32.dll")]
    private static extern bool SetInformationJobObject(
        IntPtr job,
        uint informationClass,
        IntPtr information,
        uint informationLength);

    [DllImport("kernel32.dll")]
    private static extern bool AssignProcessToJobObject(IntPtr job, IntPtr process);

    [DllImport("kernel32.dll")]
    private static extern bool CloseHandle(IntPtr handle);

    [StructLayout(LayoutKind.Sequential)]
    private struct IoCounters
    {
        internal ulong ReadOperationCount;
        internal ulong WriteOperationCount;
        internal ulong OtherOperationCount;
        internal ulong ReadTransferCount;
        internal ulong WriteTransferCount;
        internal ulong OtherTransferCount;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct JobObjectBasicLimitInformation
    {
        internal long PerProcessUserTimeLimit;
        internal long PerJobUserTimeLimit;
        internal uint LimitFlags;
        internal UIntPtr MinimumWorkingSetSize;
        internal UIntPtr MaximumWorkingSetSize;
        internal uint ActiveProcessLimit;
        internal UIntPtr Affinity;
        internal uint PriorityClass;
        internal uint SchedulingClass;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct JobObjectExtendedLimitInformation
    {
        internal JobObjectBasicLimitInformation BasicLimitInformation;
        internal IoCounters IoInfo;
        internal UIntPtr ProcessMemoryLimit;
        internal UIntPtr JobMemoryLimit;
        internal UIntPtr PeakProcessMemoryUsed;
        internal UIntPtr PeakJobMemoryUsed;
    }
}
