param(
    [string]$OutputDirectory = "results/issue29-release",
    [string]$ContainerName = "calve-issue29-release",
    [int]$Port = 8193,
    [string]$DockerExe = "docker"
)

$ErrorActionPreference = "Stop"
if (-not (Test-Path -LiteralPath "go.mod")) {
    throw "Run this script from the Calve repository root."
}
if (Test-Path -LiteralPath $OutputDirectory) {
    throw "Choose a new output directory: $OutputDirectory"
}
if ($Port -lt 1 -or $Port -gt 65535) {
    throw "Port must be between 1 and 65535."
}

$icebergRoot = (New-Item -ItemType Directory -Path $OutputDirectory).FullName
$icebergSource = Join-Path $icebergRoot "source"
$icebergLibs = (New-Item -ItemType Directory -Path (Join-Path $icebergRoot "libs")).FullName
$icebergClasses = (New-Item -ItemType Directory -Path (Join-Path $icebergRoot "classes")).FullName
$icebergJar = Join-Path $icebergRoot "fixture-1.10.1.jar"
$icebergFixture = & $DockerExe compose -p calve-reference-issue ps -q rest-fixture
if ($LASTEXITCODE -ne 0 -or -not $icebergFixture) {
    throw "Start the calve-reference-issue fixture first."
}
$icebergFixtureImage = & $DockerExe inspect $icebergFixture --format '{{.Image}}'
if ($LASTEXITCODE -ne 0 -or -not $icebergFixtureImage) {
    throw "Could not identify the running fixture image."
}

& $DockerExe cp ($icebergFixture + ":/usr/lib/iceberg-rest/iceberg-rest-adapter.jar") $icebergJar
if ($LASTEXITCODE -ne 0) { throw "Copying the fixture jar failed." }
& git clone --quiet --depth 1 --branch apache-iceberg-1.12.0 https://github.com/apache/iceberg.git $icebergSource
if ($LASTEXITCODE -ne 0) { throw "Fetching the Iceberg release source failed." }
$icebergRevision = & git -C $icebergSource rev-parse HEAD
if ($LASTEXITCODE -ne 0 -or $icebergRevision -ne "212aa894e919ff9fdf89160da4aadcbe6858ac01") {
    throw "The Iceberg release source does not match the recorded revision."
}

$icebergArtifacts = @(
    "iceberg-core", "iceberg-api", "iceberg-common", "iceberg-bundled-guava",
    "iceberg-aws", "iceberg-azure", "iceberg-gcp"
)
$icebergExpected = @{
    "iceberg-core" = "d3bbb331b7a907e134958d21ecccd91a736049e00768e07579ff60855bf28352"
    "iceberg-api" = "d476a6989edfe3882a47ce0eaa3e53df4d589ef5e7bd0a16ff113ae570bca8b6"
    "iceberg-common" = "18a43330e3c5eb712a9bf88e8f70441971851400efb4646f0b6512ac40c5f357"
    "iceberg-bundled-guava" = "8d27e319e3bc28f7f86e7fbd59f6758bfaa1f78d72d9f76aa93ad1f7cbd3ba34"
    "iceberg-aws" = "ec82373b4126e54f415768081926773b07c959eec14268e806c8e441324217a5"
    "iceberg-azure" = "62be437a171d7def2805e8cb33d6c9a70ea7fa93cfaf8d6390479a41d3981057"
    "iceberg-gcp" = "d453152c541eafcdac28df42234b6310d76e295c6770870ae967538f19a38dc0"
}
foreach ($icebergArtifact in $icebergArtifacts) {
    $icebergFilename = $icebergArtifact + "-1.12.0.jar"
    $icebergUrl = "https://repo.maven.apache.org/maven2/org/apache/iceberg/" +
        $icebergArtifact + "/1.12.0/" + $icebergFilename
    & curl.exe -fsSL --output (Join-Path $icebergLibs $icebergFilename) $icebergUrl
    if ($LASTEXITCODE -ne 0) { throw "Downloading $icebergFilename failed." }
    $icebergActual = (Get-FileHash -Algorithm SHA256 (Join-Path $icebergLibs $icebergFilename)).Hash.ToLowerInvariant()
    if ($icebergActual -ne $icebergExpected[$icebergArtifact]) {
        throw "Checksum mismatch for $icebergFilename."
    }
}

$icebergCompileJars = @($icebergArtifacts | ForEach-Object { Join-Path $icebergLibs ($_ + "-1.12.0.jar") })
$icebergCompileJars += $icebergJar
$icebergSources = @(
    "core/src/test/java/org/apache/iceberg/rest/Route.java",
    "core/src/test/java/org/apache/iceberg/rest/RESTCatalogAdapter.java",
    "core/src/test/java/org/apache/iceberg/rest/RESTCatalogServlet.java",
    "open-api/src/testFixtures/java/org/apache/iceberg/rest/RESTServerCatalogAdapter.java"
) | ForEach-Object { Join-Path $icebergSource $_ }
& javac --release 17 -cp ($icebergCompileJars -join ";") -d $icebergClasses @icebergSources
if ($LASTEXITCODE -ne 0) { throw "Compiling the 1.12.0 fixture adapter failed." }

$icebergRuntimeJars = @("/tmp/iceberg-classes")
$icebergRuntimeJars += @($icebergArtifacts | ForEach-Object { "/tmp/iceberg-libs/" + $_ + "-1.12.0.jar" })
$icebergRuntimeJars += "/usr/lib/iceberg-rest/iceberg-rest-adapter.jar"
$icebergMountLibs = "type=bind,source=" + $icebergLibs + ",target=/tmp/iceberg-libs,readonly"
$icebergMountClasses = "type=bind,source=" + $icebergClasses + ",target=/tmp/iceberg-classes,readonly"
$icebergRunArgs = @("run", "-d", "--name", $ContainerName, "-p", ($Port.ToString() + ":8181"),
    "--mount", $icebergMountLibs, "--mount", $icebergMountClasses, "--entrypoint", "java",
    $icebergFixtureImage, "-Xmx1024m", "-cp", ($icebergRuntimeJars -join ":"),
    "org.apache.iceberg.rest.RESTCatalogServer")
& $DockerExe @icebergRunArgs
if ($LASTEXITCODE -ne 0) { throw "Starting the assembled fixture failed." }

$icebergReady = $false
for ($icebergAttempt = 0; $icebergAttempt -lt 30; $icebergAttempt++) {
    try {
        $icebergResponse = Invoke-WebRequest -UseBasicParsing -TimeoutSec 2 ("http://localhost:" + $Port + "/v1/config")
        if ($icebergResponse.StatusCode -eq 200) { $icebergReady = $true; break }
    } catch { Start-Sleep -Seconds 1 }
}
if (-not $icebergReady) {
    & $DockerExe logs --tail 30 $ContainerName
    throw "The assembled fixture did not become ready."
}
$icebergImage = & $DockerExe inspect $ContainerName --format '{{.Image}}'
$icebergDigests = @($icebergArtifacts | ForEach-Object {
    $icebergFile = Join-Path $icebergLibs ($_ + "-1.12.0.jar")
    [pscustomobject]@{ artifact = $_; sha256 = (Get-FileHash -Algorithm SHA256 $icebergFile).Hash.ToLowerInvariant() }
})
[pscustomobject]@{
    source_revision = $icebergRevision
    wrapper_image = $icebergImage
    container = $ContainerName
    port = $Port
    libraries = $icebergDigests
} | ConvertTo-Json -Depth 4 | Set-Content -Encoding UTF8 (Join-Path $icebergRoot "assembly.json")
Write-Output "Assembled fixture ready at http://localhost:$Port"
Write-Output "Stop it with: docker stop $ContainerName"
