# Taskfile for bobbob project - equivalent to Python's tox
# Usage: ./task.ps1 [command]

param(
    [Parameter(Position=0)]
    [string]$Command = "help"
)

$tasks = @{
    "help" = "Display this help message"
    "test" = "Run all tests"
    "test-race" = "Run tests with race condition detection"
    "test-coverage" = "Run tests with coverage report"
    "lint" = "Run linting checks (errcheck)"
    "fmt" = "Format code (gofmt)"
    "fmt-check" = "Check if code needs formatting"
    "build" = "Build the project"
    "clean" = "Clean build artifacts"
    "check" = "Run all checks (format check, lint, test) - CI/CD friendly"
    "all" = "Run all checks: format, lint, and test"
}

function Show-Help {
    Write-Host "Available tasks:" -ForegroundColor Cyan
    $tasks.GetEnumerator() | Sort-Object Name | ForEach-Object {
        Write-Host ("  {0,-20} {1}" -f $_.Name, $_.Value)
    }
    Write-Host ""
    Write-Host "Usage: ./dev.ps1 [command]" -ForegroundColor Yellow
    Write-Host "Example: ./dev.ps1 test"
}

function Invoke-Test {
    Write-Host "Running tests..." -ForegroundColor Green
    go test -v ./...
    if ($LASTEXITCODE -ne 0) { exit 1 }
}

function Invoke-TestRace {
    Write-Host "Running tests with race detection..." -ForegroundColor Green
    go test -v -race ./...
    if ($LASTEXITCODE -ne 0) { exit 1 }
}

function Invoke-TestCoverage {
    Write-Host "Running tests with coverage..." -ForegroundColor Green
    go test -v -coverprofile=coverage.out ./...
    if ($LASTEXITCODE -ne 0) { exit 1 }
    go tool cover -html=coverage.out -o coverage.html
    Write-Host "Coverage report: coverage.html" -ForegroundColor Green
}

function Invoke-Lint {
    Write-Host "Running errcheck (production code only)..." -ForegroundColor Green
    # errcheck on production code only (./... would include test files)
    # Test cleanup errors (os.Remove, Close, finisher, etc.) are acceptable
    foreach ($pkg in @("./chain", "./cmd", "./internal/testutil", "./multistore", "./store", "./yggdrasil")) {
        errcheck "$pkg" 2>&1 | Where-Object { $_ -notmatch "_test\.go" } | ForEach-Object {
            if ($_) { Write-Host $_; exit 1 }
        }
    }
    Write-Host "errcheck passed" -ForegroundColor Green
    
    Write-Host "Running govulncheck..." -ForegroundColor Green
    $vulnOutput = govulncheck ./... 2>&1
    $hasNonStdlibVuln = $false
    $vulnOutput | ForEach-Object {
        if ($_ -match "Vulnerability #" -and $_ -notmatch "GO-2025-3750") {
            $hasNonStdlibVuln = $true
        }
    }
    if ($hasNonStdlibVuln) {
        Write-Host "Non-stdlib vulnerability found:" -ForegroundColor Red
        exit 1
    }
    Write-Host "govulncheck passed" -ForegroundColor Green
}

function Invoke-Fmt {
    Write-Host "Formatting code..." -ForegroundColor Green
    go fmt ./...
    Write-Host "Code formatted" -ForegroundColor Green
}

function Invoke-FmtCheck {
    Write-Host "Checking code format..." -ForegroundColor Green
    $files = go fmt ./... 2>&1
    if ($files) {
        Write-Host "Code needs formatting:" -ForegroundColor Red
        Write-Host $files
        Write-Host "Run './task.ps1 fmt' to fix" -ForegroundColor Yellow
        exit 1
    }
    Write-Host "Code format OK" -ForegroundColor Green
}

function Invoke-Build {
    Write-Host "Building project..." -ForegroundColor Green
    go build ./...
    if ($LASTEXITCODE -ne 0) { exit 1 }
    Write-Host "Build successful" -ForegroundColor Green
}

function Invoke-Clean {
    Write-Host "Cleaning build artifacts..." -ForegroundColor Green
    go clean -testcache
    Remove-Item -Force -ErrorAction SilentlyContinue coverage.out, coverage.html
    Write-Host "Clean complete" -ForegroundColor Green
}

function Invoke-Check {
    Write-Host "Running full check (format check, lint, test)..." -ForegroundColor Cyan
    Invoke-FmtCheck
    Invoke-Lint
    Invoke-Test
    Write-Host "All checks passed!" -ForegroundColor Green
}

function Invoke-All {
    Write-Host "Running all tasks (format, lint, test)..." -ForegroundColor Cyan
    Invoke-Fmt
    Invoke-Lint
    Invoke-Test
    Write-Host "All tasks completed!" -ForegroundColor Green
}

# Execute the requested command
switch ($Command) {
    "help" { Show-Help }
    "test" { Invoke-Test }
    "test-race" { Invoke-TestRace }
    "test-coverage" { Invoke-TestCoverage }
    "lint" { Invoke-Lint }
    "fmt" { Invoke-Fmt }
    "fmt-check" { Invoke-FmtCheck }
    "build" { Invoke-Build }
    "clean" { Invoke-Clean }
    "check" { Invoke-Check }
    "all" { Invoke-All }
    default {
        Write-Host "Unknown command: $Command" -ForegroundColor Red
        Show-Help
        exit 1
    }
}
