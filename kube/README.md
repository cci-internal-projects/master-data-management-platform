# MDM Platform Kubernetes manifests

This directory contains the Django MDM application resources for the BYPL TnD
cluster. Airflow, Keycloak, PostgreSQL, Redis, the namespace, Traefik, TLS
issuance, and the container registry are managed separately.

## Configuration ownership

- `configmap.yaml` is the single place for non-sensitive runtime and BYPL
  deployment settings.
- `secret.yaml` contains passwords and the Django secret. It is intentionally
  ignored by Git.
- `secret.example.yaml` documents the required Secret keys without containing
  usable credentials.
- `kustomization.yaml` selects the namespace, replica count, registry image,
  and image tag once for both the Deployment and migration Job.

Before rendering, replace every `replace-with-...` value in `configmap.yaml`
and `secret.yaml`. Do not commit `secret.yaml`.

## Build and publish the image

The current manifests select image tag `1.0.2`:

```powershell
docker build -t k8s-local-cont-regd.local:5000/cuculus/mdm-platform:1.0.2 .
docker push k8s-local-cont-regd.local:5000/cuculus/mdm-platform:1.0.2
```

If the registry or tag changes, update only the `images` entry in
`kustomization.yaml`. Keep the migration Job name aligned with releases that
must execute migrations. A completed Job's pod template is immutable, so give
the Job a new name before applying a later migration-bearing release through
Flux.

## Prepare the local Secret

```powershell
Copy-Item kube\secret.example.yaml kube\secret.yaml
```

Edit `kube/secret.yaml` locally and supply approved BYPL values. The namespace
must already contain the image-pull Secret named by `IMAGE_PULL_SECRET` and the
TLS Secret named by `INGRESS_TLS_SECRET`.

## Validate and apply

Render without contacting the cluster:

```powershell
kubectl kustomize kube
& .\tests\kube-manifests.tests.ps1
```

Review the rendered output before applying it:

```powershell
kubectl apply -k kube
```

For Flux delivery, copy or mirror this directory into the GitRepository that
the BYPL Flux installation reconciles and point its Flux Kustomization at that
path. Merely pushing these files to the application source repository will not
change the cluster unless Flux watches this repository.
