$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$kubeDirectory = Join-Path $repoRoot "kube"

$rendered = & kubectl kustomize $kubeDirectory 2>&1
if ($LASTEXITCODE -ne 0) {
    throw "kubectl kustomize failed:`n$($rendered -join [Environment]::NewLine)"
}

$yaml = $rendered -join [Environment]::NewLine
$documents = $yaml -split "(?m)^---\s*$" | Where-Object { $_.Trim() }

function Get-ManifestDocument {
    param(
        [Parameter(Mandatory)] [string] $Kind,
        [Parameter(Mandatory)] [string] $Name
    )

    $matches = @(
        $documents | Where-Object {
            $_ -match "(?m)^kind:\s+$([regex]::Escape($Kind))\s*$" -and
            $_ -match "(?m)^\s*name:\s+$([regex]::Escape($Name))\s*$"
        }
    )

    if ($matches.Count -ne 1) {
        throw "Expected exactly one $Kind/$Name manifest, found $($matches.Count)."
    }

    return $matches[0]
}

$configMap = Get-ManifestDocument -Kind "ConfigMap" -Name "mdm-platform-config"
$secret = Get-ManifestDocument -Kind "Secret" -Name "mdm-platform-application"
$deployment = Get-ManifestDocument -Kind "Deployment" -Name "mdm-platform"
$service = Get-ManifestDocument -Kind "Service" -Name "mdm-platform"
$ingress = Get-ManifestDocument -Kind "Ingress" -Name "mdm-platform"
$migrationJob = Get-ManifestDocument -Kind "Job" -Name "mdm-platform-migrate-v1-0-2"

$forbiddenConfigKeys = @(
    "SECRET_KEY",
    "POSTGRES_PASSWORD",
    "ADMIN_PASSWORD",
    "ZONOS_NORTHBOUND_PASSWORD"
)

foreach ($key in $forbiddenConfigKeys) {
    if ($configMap -match "(?m)^\s+$([regex]::Escape($key)):") {
        throw "$key must not be stored in the ConfigMap."
    }

    if ($secret -notmatch "(?m)^\s+$([regex]::Escape($key)):") {
        throw "$key must be supplied by the application Secret."
    }
}

$requiredConfigKeys = @(
    "ALLOWED_HOSTS",
    "CSRF_TRUSTED_ORIGINS",
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "REDIS_URL",
    "ZONOS_NORTHBOUND_BASE_URL",
    "ZONOS_NORTHBOUND_TOKEN_URL",
    "ZONOS_NORTHBOUND_USERNAME",
    "ADMIN_USERNAME",
    "ADMIN_EMAIL"
)

foreach ($key in $requiredConfigKeys) {
    if ($configMap -notmatch "(?m)^\s+$([regex]::Escape($key)):") {
        throw "$key must be supplied by the ConfigMap."
    }
}

foreach ($workload in @($deployment, $migrationJob)) {
    if ($workload -notmatch "(?m)^\s+name:\s+mdm-platform-config\s*$") {
        throw "Each workload must consume mdm-platform-config."
    }
    if ($workload -notmatch "(?m)^\s+name:\s+mdm-platform-application\s*$") {
        throw "Each workload must consume mdm-platform-application."
    }
    if ($workload -notmatch "k8s-local-cont-regd\.local:5000/cuculus/mdm-platform:1\.0\.2") {
        throw "Each workload must use the centrally selected MDM image tag."
    }
}

if ($service -notmatch "(?m)^\s+port:\s+8009\s*$" -or
    $service -notmatch "(?m)^\s+targetPort:\s+http\s*$") {
    throw "The Service must expose port 8009 and target the named HTTP container port."
}

if ($ingress -notmatch "(?m)^\s+-?\s*host:\s+byplmdmsdev-mdm-platform\.bsesdelhi\.com\s*$" -or
    $ingress -notmatch "(?m)^\s+secretName:\s+bypl-certificate\s*$") {
    throw "The rendered Ingress must receive its BYPL host and TLS Secret from deployment configuration."
}

if ($documents.Count -ne 6) {
    throw "Expected 6 rendered Kubernetes resources, found $($documents.Count)."
}

Write-Output "Kubernetes manifest contract passed: 6 resources rendered with ConfigMap and Secret separation."
