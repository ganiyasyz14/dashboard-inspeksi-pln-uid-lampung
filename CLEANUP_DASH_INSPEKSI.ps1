param(
    [ValidateSet('Preview','Clean')]
    [string]$Mode = 'Preview',
    [switch]$IncludeVenv,
    [switch]$IncludeQuickGSCheck,
    [switch]$IncludeDocs,
    [switch]$Yes
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $MyInvocation.MyCommand.Path

function Get-ItemSizeBytes([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path)) { return 0 }
    $item = Get-Item -LiteralPath $Path -Force -ErrorAction SilentlyContinue
    if ($item.PSIsContainer) {
        try {
            return (Get-ChildItem -LiteralPath $Path -Recurse -Force -ErrorAction SilentlyContinue | Measure-Object -Property Length -Sum).Sum
        } catch { return 0 }
    } else {
        return ($item | Measure-Object -Property Length -Sum).Sum
    }
}

# Define candidates (do NOT touch credentials.json, assets/, .streamlit/, app.py, sheets_utils.py, start_dashboard.bat, requirements.txt, README.md)
$candidates = @(
    @{ Name = '__pycache__'; Path = Join-Path $root '__pycache__'; Kind = 'Directory'; Include = $true },
    @{ Name = '.cache'; Path = Join-Path $root '.cache'; Kind = 'Directory'; Include = $true },
    @{ Name = 'snapshot_manager.py'; Path = Join-Path $root 'snapshot_manager.py'; Kind = 'File'; Include = $true },
    @{ Name = 'create_ppt_dashboard.py'; Path = Join-Path $root 'create_ppt_dashboard.py'; Kind = 'File'; Include = $true },
    # Optional candidates (off unless switches supplied)
    @{ Name = 'quick_gs_check.py'; Path = Join-Path $root 'quick_gs_check.py'; Kind = 'File'; Include = $IncludeQuickGSCheck.IsPresent },
    @{ Name = 'LOGBOOK_MAGANG_MBKM.md'; Path = Join-Path $root 'LOGBOOK_MAGANG_MBKM.md'; Kind = 'File'; Include = $IncludeDocs.IsPresent },
    @{ Name = 'venv'; Path = Join-Path $root 'venv'; Kind = 'Directory'; Include = $IncludeVenv.IsPresent }
)

$toProcess = @()
foreach ($c in $candidates) {
    if (-not $c.Include) { continue }
    if (-not (Test-Path -LiteralPath $c.Path)) { continue }
    $sizeBytes = Get-ItemSizeBytes -Path $c.Path
    $obj = [pscustomobject]@{
        Name  = $c.Name
        Kind  = $c.Kind
        SizeMB = [math]::Round(($sizeBytes/1MB),2)
        Path  = $c.Path
    }
    $toProcess += $obj
}

if ($Mode -eq 'Preview') {
    if ($toProcess.Count -eq 0) {
        Write-Host '[Preview] Tidak ada kandidat file/folder untuk dibersihkan.' -ForegroundColor Yellow
        Write-Host 'Tip: Gunakan switch -IncludeVenv / -IncludeQuickGSCheck / -IncludeDocs jika ingin memasukkan item opsional.'
        exit 0
    }
    Write-Host '=== PREVIEW: Kandidat cleanup (tidak ada yang dihapus) ===' -ForegroundColor Cyan
    $toProcess | Sort-Object Kind, Name | Format-Table Name, Kind, SizeMB, Path -AutoSize
    $totalMB = ($toProcess | Measure-Object -Property SizeMB -Sum).Sum
    Write-Host ("Total perkiraan ukuran: {0} MB" -f ([math]::Round($totalMB,2))) -ForegroundColor DarkCyan
    Write-Host ''
    Write-Host 'Untuk menjalankan pembersihan:' -ForegroundColor Green
    $scriptPath = Join-Path $root 'CLEANUP_DASH_INSPEKSI.ps1'
    $incVenv  = if ($IncludeVenv) { ' -IncludeVenv' } else { '' }
    $incQuick = if ($IncludeQuickGSCheck) { ' -IncludeQuickGSCheck' } else { '' }
    $incDocs  = if ($IncludeDocs) { ' -IncludeDocs' } else { '' }
    $cmd = "powershell -NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`" -Mode Clean$incVenv$incQuick$incDocs -Yes"
    Write-Host $cmd -ForegroundColor White
    exit 0
}

# Clean mode
if ($toProcess.Count -eq 0) {
    Write-Host '[Clean] Tidak ada kandidat untuk dihapus.' -ForegroundColor Yellow
    exit 0
}

Write-Host '=== CLEAN: Item yang akan dihapus ===' -ForegroundColor Red
$toProcess | Sort-Object Kind, Name | Format-Table Name, Kind, SizeMB, Path -AutoSize

if (-not $Yes) {
    $confirm = Read-Host 'Lanjutkan hapus? ketik Y untuk konfirmasi'
    if ($confirm -notin @('Y','y')) {
        Write-Host 'Dibatalkan.' -ForegroundColor Yellow
        exit 1
    }
}

foreach ($item in $toProcess) {
    try {
        if (Test-Path -LiteralPath $item.Path) {
            if ($item.Kind -eq 'Directory') {
                Remove-Item -LiteralPath $item.Path -Recurse -Force -ErrorAction Continue
            } else {
                Remove-Item -LiteralPath $item.Path -Force -ErrorAction Continue
            }
            Write-Host ("[OK] Dihapus: {0}" -f $item.Path) -ForegroundColor Green
        }
    } catch {
        Write-Host ("[SKIP] Gagal menghapus: {0} => {1}" -f $item.Path, $_.Exception.Message) -ForegroundColor Yellow
    }
}

Write-Host 'Selesai.' -ForegroundColor Green
