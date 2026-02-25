# Admin Portal (Blob + Postgres ingest manager)

Small Quart web app to upload, list, and delete documents for the main RAG app.

## What it manages
- **Azure Blob Storage**: original uploaded file and extracted page images (optional).
- **Postgres (`documents` table)**: chunks + embeddings, keyed by `doc_id`.

## Prereqs

- Python 3.10+ (repo uses 3.12 fine)
- Environment variables configured (same as `r7jibungotoka/scripts/ingest_pgvector.py`)
  - Postgres: `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE` (+ optional `PGSSLMODE`)
  - Azure OpenAI: `AZURE_OPENAI_API_VERSION`, `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_KEY`, `AZURE_OPENAI_EMBEDDING_MODEL`
  - Azure Document Intelligence: `AZURE_DI_ENDPOINT`, `AZURE_DI_KEY`
  - Blob storage (either):
    - `AZURE_BLOB_CONNECTION_STRING` (+ optional `AZURE_BLOB_CONTAINER_NAME`), or
    - `AZURE_STORAGE_ACCOUNT_NAME` + `AZURE_STORAGE_ACCOUNT_KEY` (+ optional `AZURE_BLOB_CONTAINER`)
  - (Optional) Azure Container Apps Job control (enables the “スクレイパーを実行” button on `/sites`):
    - `ACA_SUBSCRIPTION_ID`, `ACA_RESOURCE_GROUP`, `ACA_JOB_NAME`
    - Auth (choose one):
      - App Service Managed Identity (recommended): enable system-assigned MI, or set `ACA_MANAGED_IDENTITY_CLIENT_ID` for user-assigned MI
      - Service principal: `ACA_ARM_TENANT_ID`, `ACA_ARM_CLIENT_ID`, `ACA_ARM_CLIENT_SECRET`

## Run locally
```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r adminportal/requirements.txt

export ADMIN_USERID=admin   # or ADMIN_USERNAME (legacy)
export ADMIN_PASSWORD=change-me
export ADMINPORTAL_SECRET_KEY='change-this-to-a-long-random-string'
quart --app adminportal.app run --port 5055 --reload
```

Open `http://localhost:5055`.

## Deploy to Azure (App Service, ~10 users)
Recommended for a small internal admin app: **Azure App Service (Linux)** on **Basic B1** (or Free F1 for testing).

### 1) Create App Service
From the `adminportal/` folder:
```bash
az login
az group create -n flood-rg -l eastus
az appservice plan create -g flood-rg -n flood-admin-plan --is-linux --sku B1
az webapp create -g flood-rg -p flood-admin-plan -n <unique-app-name> --runtime "PYTHON|3.12"
```

### 2) Configure startup + env vars
Set the startup command to run Quart via Hypercorn:
```bash
az webapp config set -g flood-rg -n <unique-app-name> --startup-file "bash startup.sh"
```

Set required environment variables (secrets) in App Settings (Portal) or CLI:
```bash
az webapp config appsettings set -g flood-rg -n <unique-app-name> --settings \
  SCM_DO_BUILD_DURING_DEPLOYMENT=1 \
  ADMINPORTAL_SESSION_COOKIE_SECURE=1 \
  ADMIN_USERID=admin \
  ADMIN_PASSWORD=change-me \
  ADMINPORTAL_SECRET_KEY='change-this-to-a-long-random-string'
```
Also set the Azure/DB variables listed in **Prereqs** above.

If you use ACA job control in App Service:
- Enable a Managed Identity on the Web App and grant it permissions to start/read the ACA job (RBAC on the subscription/resource group).
- If you use only a user-assigned identity, set `ACA_MANAGED_IDENTITY_CLIENT_ID` to that identity’s client ID.

### 3) Deploy code
Quick zip deploy:
```bash
zip -r deploy.zip . -x ".venv/*" "__pycache__/*"
az webapp deployment source config-zip -g flood-rg -n <unique-app-name> --src deploy.zip
```

### 4) Verify
Open `https://<unique-app-name>.azurewebsites.net` and log in.

### GitHub Actions (CI deploy)
This repo includes a workflow at `.github/workflows/deploy-azure-appservice.yml`.

1) In Azure Portal → your Web App → **Get publish profile** → download the file.
2) In GitHub → your repo → **Settings → Secrets and variables → Actions**:
   - Add secret `AZUREAPPSERVICE_PUBLISHPROFILE` with the publish profile XML contents.
3) Edit `.github/workflows/deploy-azure-appservice.yml` and set `AZURE_WEBAPP_NAME` to your app name.
4) Push to `main` to deploy (or run it manually via **Actions → workflow_dispatch**).

## Notes
- Upload triggers ingest into Postgres automatically (ADI extraction + embeddings + upsert).
- Delete removes:
  - all rows from `documents` with `doc_id = <filename>`
  - the blob `<filename>`
  - any blobs under `images/<filename>/...` (if present)
