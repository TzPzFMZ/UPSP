using System.Diagnostics;
using System.Runtime.InteropServices;
using Microsoft.Web.WebView2.Core;
using Microsoft.Web.WebView2.WinForms;

namespace UPSP.Desktop;

internal sealed class MainForm : Form
{
    private readonly DesktopBackend _backend;
    private readonly WebView2 _webView = new() { Dock = DockStyle.Fill };
    private readonly Icon? _appIcon;
    private readonly NotifyIcon _tray;
    private readonly ToolStripMenuItem _statusItem = new("当前状态：正在连接");
    private readonly System.Windows.Forms.Timer _statusTimer = new() { Interval = 2000 };
    private bool _allowClose;
    private bool _exitInProgress;
    private bool _backendFailureShown;
    private bool _lastActive;
    private int _statusRefreshInFlight;

    internal MainForm(DesktopBackend backend)
    {
        _backend = backend;
        _appIcon = Icon.ExtractAssociatedIcon(Application.ExecutablePath);
        Text = "UPSP";
        Icon = _appIcon ?? SystemIcons.Application;
        StartPosition = FormStartPosition.CenterScreen;
        ClientSize = new Size(1400, 900);
        MinimumSize = new Size(760, 560);
        BackColor = Color.FromArgb(5, 20, 18);
        Controls.Add(_webView);

        var menu = new ContextMenuStrip();
        menu.Items.Add("打开 UPSP", null, (_, _) => RestoreFromTray());
        _statusItem.Enabled = false;
        menu.Items.Add(_statusItem);
        menu.Items.Add(new ToolStripSeparator());
        menu.Items.Add("退出", null, async (_, _) => await TryExitAsync());

        _tray = new NotifyIcon
        {
            Icon = Icon,
            Text = "UPSP",
            ContextMenuStrip = menu,
            Visible = true,
        };
        _tray.DoubleClick += (_, _) => RestoreFromTray();
        FormClosing += OnFormClosing;
        Shown += OnShown;
        _statusTimer.Tick += async (_, _) => await RefreshStatusAsync();
    }

    internal void RestoreFromTray()
    {
        if (WindowState == FormWindowState.Minimized)
        {
            WindowState = FormWindowState.Normal;
        }
        Show();
        Activate();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _statusTimer.Dispose();
            _tray.Visible = false;
            _tray.Dispose();
            _appIcon?.Dispose();
            _webView.Dispose();
        }
        base.Dispose(disposing);
    }

    private async void OnShown(object? sender, EventArgs args)
    {
        UseDarkTitleBar();
        try
        {
            var userData = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                "UPSP",
                "cache",
                "webview2");
            Directory.CreateDirectory(userData);
            var environment = await CoreWebView2Environment.CreateAsync(null, userData);
            await _webView.EnsureCoreWebView2Async(environment);
            ConfigureWebView(_webView.CoreWebView2);
            _webView.Source = _backend.Origin;
            _statusTimer.Start();
            await RefreshStatusAsync();
        }
        catch (Exception exc)
        {
            DesktopLog.Write(exc);
            MessageBox.Show(
                $"UPSP 窗口初始化失败。\n\n{exc.Message}",
                "UPSP",
                MessageBoxButtons.OK,
                MessageBoxIcon.Error);
            await ShutdownAfterStartupFailureAsync();
        }
    }

    private void ConfigureWebView(CoreWebView2 core)
    {
        core.Settings.AreDevToolsEnabled = false;
        core.Settings.AreHostObjectsAllowed = false;
        core.NavigationStarting += (_, args) =>
        {
            if (AllowedTopLevel(args.Uri))
            {
                return;
            }
            args.Cancel = true;
            OpenExternal(args.Uri);
        };
        core.NewWindowRequested += (_, args) =>
        {
            args.Handled = true;
            OpenExternal(args.Uri);
        };
        core.DownloadStarting += (_, args) =>
        {
            args.Handled = true;
            if (!args.DownloadOperation.Uri.StartsWith(
                $"blob:{_backend.Origin.GetLeftPart(UriPartial.Authority)}",
                StringComparison.OrdinalIgnoreCase))
            {
                args.Cancel = true;
                return;
            }
            using var dialog = new SaveFileDialog
            {
                AddExtension = true,
                DefaultExt = "json",
                Filter = "JSON 文件 (*.json)|*.json|所有文件 (*.*)|*.*",
                FileName = "UPSP-evidence.json",
                OverwritePrompt = true,
            };
            if (dialog.ShowDialog(this) == DialogResult.OK)
            {
                args.ResultFilePath = dialog.FileName;
            }
            else
            {
                args.Cancel = true;
            }
        };
    }

    private bool AllowedTopLevel(string value)
    {
        if (value == "about:blank")
        {
            return true;
        }
        return Uri.TryCreate(value, UriKind.Absolute, out var uri)
            && uri.GetLeftPart(UriPartial.Authority).Equals(
                _backend.Origin.GetLeftPart(UriPartial.Authority),
                StringComparison.OrdinalIgnoreCase);
    }

    private static void OpenExternal(string value)
    {
        if (!Uri.TryCreate(value, UriKind.Absolute, out var uri)
            || uri.Scheme is not ("http" or "https"))
        {
            return;
        }
        Process.Start(new ProcessStartInfo(uri.AbsoluteUri) { UseShellExecute = true });
    }

    private async Task RefreshStatusAsync()
    {
        if (Interlocked.Exchange(ref _statusRefreshInFlight, 1) != 0)
        {
            return;
        }
        try
        {
            var snapshot = await _backend.GetStatusAsync();
            if (!snapshot.Connected && _backend.HasExited)
            {
                _statusItem.Text = "当前状态：后端已停止";
                ShowBackendFailure(
                    "UPSP 本地后端进程已经退出。程序不会自动重启它，请退出后重新打开 UPSP。");
                return;
            }
            if (snapshot.Connected)
            {
                _backendFailureShown = false;
            }
            _statusItem.Text = $"当前状态：{snapshot.DisplayText}";
            if (_lastActive && !snapshot.HasActiveOperation)
            {
                NotifyCompletion(snapshot);
            }
            _lastActive = snapshot.HasActiveOperation;
        }
        catch (Exception exc)
        {
            DesktopLog.Write(exc);
            _statusItem.Text = "当前状态：后端繁忙，正在重试";
            if (!_backendFailureShown && _backend.HasExited)
            {
                ShowBackendFailure(
                    "UPSP 本地后端进程已经退出。程序不会自动重启它，请退出后重新打开 UPSP。");
            }
        }
        finally
        {
            Interlocked.Exchange(ref _statusRefreshInFlight, 0);
        }
    }

    private void ShowBackendFailure(string message)
    {
        if (_backendFailureShown)
        {
            return;
        }
        _backendFailureShown = true;
        MessageBox.Show(
            message,
            "UPSP",
            MessageBoxButtons.OK,
            MessageBoxIcon.Error);
    }

    private void NotifyCompletion(RuntimeSnapshot snapshot)
    {
        if (Visible)
        {
            return;
        }
        var message = snapshot.Outcome == "round_stopped"
            ? "当前轮次已停止。"
            : snapshot.Settlement == "unsettled"
                ? "当前轮次运行失败，请打开 UPSP 查看状态。"
                : "当前轮次已完成。";
        _tray.ShowBalloonTip(4000, "UPSP", message, ToolTipIcon.Info);
    }

    private void OnFormClosing(object? sender, FormClosingEventArgs args)
    {
        if (_allowClose)
        {
            return;
        }
        if (args.CloseReason == CloseReason.UserClosing)
        {
            args.Cancel = true;
            Hide();
        }
    }

    private async Task TryExitAsync()
    {
        if (_exitInProgress)
        {
            return;
        }
        _exitInProgress = true;
        try
        {
            var snapshot = await _backend.GetStatusAsync();
            if (snapshot.HasActiveOperation)
            {
                var answer = MessageBox.Show(
                    "当前轮次仍在运行。退出会停止模型请求，并等待本地善后完成。\n\n是否继续退出？",
                    "退出 UPSP",
                    MessageBoxButtons.YesNo,
                    MessageBoxIcon.Warning,
                    MessageBoxDefaultButton.Button2);
                if (answer != DialogResult.Yes)
                {
                    return;
                }
                var stopAccepted = await _backend.RequestStopAsync();
                snapshot = await _backend.WaitForIdleAsync(TimeSpan.FromSeconds(90));
                if (snapshot.HasActiveOperation
                    || stopAccepted && !snapshot.StopOutcomeSafe)
                {
                    MessageBox.Show(
                        "本地善后尚未完成或结算失败。UPSP 将继续留在托盘中，请打开程序查看状态。",
                        "暂时无法退出",
                        MessageBoxButtons.OK,
                        MessageBoxIcon.Warning);
                    return;
                }
            }
            if (!await _backend.ShutdownAsync())
            {
                MessageBox.Show(
                    "本地后端未能安全关闭。UPSP 将继续留在托盘中。",
                    "暂时无法退出",
                    MessageBoxButtons.OK,
                    MessageBoxIcon.Warning);
                return;
            }
            _allowClose = true;
            _tray.Visible = false;
            Application.Exit();
        }
        catch (Exception exc)
        {
            DesktopLog.Write(exc);
            MessageBox.Show(
                $"退出失败，UPSP 将继续运行。\n\n{exc.Message}",
                "UPSP",
                MessageBoxButtons.OK,
                MessageBoxIcon.Error);
        }
        finally
        {
            _exitInProgress = false;
        }
    }

    private async Task ShutdownAfterStartupFailureAsync()
    {
        if (!await _backend.ShutdownAsync())
        {
            DesktopLog.Write("Backend did not finish a graceful shutdown after WebView2 failure.");
        }
        _allowClose = true;
        Close();
    }

    private void UseDarkTitleBar()
    {
        var enabled = 1;
        _ = DwmSetWindowAttribute(Handle, 20, ref enabled, sizeof(int));
    }

    [DllImport("dwmapi.dll")]
    private static extern int DwmSetWindowAttribute(
        IntPtr window,
        int attribute,
        ref int value,
        int size);
}
