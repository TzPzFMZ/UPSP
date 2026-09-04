using System.Threading;

namespace UPSP.Desktop;

internal static class Program
{
    private const string MutexName = @"Local\UPSP.Desktop.SingleInstance.v1";
    private const string ActivateEventName = @"Local\UPSP.Desktop.Activate.v1";

    [STAThread]
    private static void Main()
    {
        Application.SetHighDpiMode(HighDpiMode.PerMonitorV2);
        Application.EnableVisualStyles();
        Application.SetCompatibleTextRenderingDefault(false);

        using var mutex = new Mutex(false, MutexName, out var firstInstance);
        using var activateEvent = new EventWaitHandle(
            false,
            EventResetMode.AutoReset,
            ActivateEventName);
        if (!firstInstance)
        {
            activateEvent.Set();
            return;
        }

        using var backend = new DesktopBackend(AppContext.BaseDirectory);
        try
        {
            backend.StartAsync().GetAwaiter().GetResult();
        }
        catch (Exception exc)
        {
            DesktopLog.Write(exc);
            MessageBox.Show(
                $"UPSP 本地后端启动失败。\n\n{exc.Message}",
                "UPSP",
                MessageBoxButtons.OK,
                MessageBoxIcon.Error);
            return;
        }

        using var form = new MainForm(backend);
        _ = form.Handle;
        var activationThread = new Thread(() =>
        {
            while (!form.IsDisposed)
            {
                try
                {
                    activateEvent.WaitOne();
                    if (!form.IsDisposed)
                    {
                        form.BeginInvoke(form.RestoreFromTray);
                    }
                }
                catch (ObjectDisposedException)
                {
                    return;
                }
                catch (InvalidOperationException)
                {
                    return;
                }
            }
        })
        {
            IsBackground = true,
            Name = "UPSP window activation",
        };
        activationThread.Start();
        Application.Run(form);
    }
}
