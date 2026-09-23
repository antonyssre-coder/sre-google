$ErrorActionPreference = 'Stop'

$node = (Get-Command node.exe -ErrorAction SilentlyContinue).Source
if (-not $node) {
  $candidates = @(
    (Join-Path $env:ProgramFiles 'nodejs\node.exe'),
    'C:\Program Files\Adobe\Adobe Creative Cloud Experience\libs\node.exe'
  )
  $node = $candidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
}
if (-not $node) { throw 'Node.js was not found. Install Node.js to rebuild the presentation.' }

if (-not $env:HOME) { $env:HOME = $env:USERPROFILE }
& $node (Join-Path $PSScriptRoot 'build_deck.mjs')
if ($LASTEXITCODE -ne 0) { throw "Presentation build failed with exit code $LASTEXITCODE." }

$builtDeck = Join-Path $env:TEMP 'Kubernetes_Observability_Automation_5slides-rebuilt.pptx'
$targetDeck = Join-Path $PSScriptRoot 'Kubernetes_Observability_Automation_5slides.pptx'
if (-not (Test-Path -LiteralPath $builtDeck)) { throw "Build output not found: $builtDeck" }
Copy-Item -LiteralPath $builtDeck -Destination $targetDeck -Force
Remove-Item -LiteralPath $builtDeck -Force
Remove-Item -LiteralPath "$builtDeck.inspect.ndjson" -Force -ErrorAction SilentlyContinue
Write-Output "Updated $targetDeck"
