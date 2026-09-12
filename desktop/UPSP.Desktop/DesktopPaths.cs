using System;
using System.IO;

namespace UPSP.Desktop;

internal static class DesktopPaths
{
    internal static string LocalStateRoot => ResolveLocalStateRoot(
        Environment.GetEnvironmentVariable("UPSP_LOCAL_STATE_ROOT"),
        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData));

    internal static string ResolveLocalStateRoot(string? configured, string localAppData)
    {
        var root = string.IsNullOrWhiteSpace(configured)
            ? Path.Combine(localAppData, "UPSP") : configured.Trim();
        if (!Path.IsPathFullyQualified(root))
        {
            throw new InvalidOperationException("local_state_root_must_be_absolute");
        }
        return Path.GetFullPath(root);
    }
}
