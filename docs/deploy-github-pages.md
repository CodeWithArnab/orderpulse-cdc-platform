# Deploy OrderPulse free with GitHub Pages

GitHub Pages hosts the public HTML/CSS/JavaScript dashboard. GitHub Actions runs the Python tests and demo generator first, then packages the generated Bronze/Silver/Gold/quarantine results into the site.

The public site is a safe, read-only portfolio demonstration. It does not keep PostgreSQL, Kafka, Debezium, or Spark running in the cloud. Those services remain reproducible through the repository's Docker configuration.

## 1. Create a GitHub repository

1. Sign in to GitHub.
2. Choose **New repository**.
3. Name it `orderpulse-cdc-platform`.
4. Set it to **Public** for free GitHub Pages hosting.
5. Do not add a README, license, or `.gitignore`; the local project already contains them.
6. Create the repository and copy its HTTPS URL.

The URL will look like:

```text
https://github.com/YOUR-USERNAME/orderpulse-cdc-platform.git
```

## 2. Push this local project

Open PowerShell in the project directory and replace `YOUR-USERNAME`:

```powershell
git init
git branch -M main
git add .
git commit -m "Build OrderPulse CDC data platform"
git remote add origin https://github.com/YOUR-USERNAME/orderpulse-cdc-platform.git
git push -u origin main
```

GitHub may open a browser or request authentication. Use your GitHub sign-in or a personal access token; do not put a token into a project file.

## 3. Enable Pages

In the GitHub repository:

1. Open **Settings**.
2. Open **Pages** under Code and automation.
3. Under **Build and deployment**, select **GitHub Actions** as the source.
4. Open the **Actions** tab.
5. Select the `deploy-pages` workflow.
6. If it has not started automatically, choose **Run workflow** on `main`.

The workflow will:

1. install the Python project;
2. run all tests;
3. regenerate the demo data;
4. build the static site artifact; and
5. deploy it to GitHub Pages.

## 4. Share the URL

The project URL will normally be:

```text
https://YOUR-USERNAME.github.io/orderpulse-cdc-platform/
```

The exact deployed URL also appears in the workflow's `deploy` job and in **Settings → Pages**.

## 5. How updates work

Every push to `main` runs `.github/workflows/pages.yml`. A deployment occurs only if the tests and demo generation succeed.

```powershell
git add .
git commit -m "Explain the improvement"
git push
```

## What visitors can and cannot do

Visitors can:

- explore the architecture;
- inspect pipeline counts and SLOs;
- see the Gold data product;
- inspect quarantined records;
- open the source repository and documentation.

Visitors cannot:

- insert real PostgreSQL orders;
- start Kafka or Spark from the web page;
- change the deployed datasets permanently.

A continuously running public Kafka/Spark backend requires a paid or time-limited cloud environment. For a portfolio, the free static demonstration plus reproducible Docker stack is usually a better and safer presentation.

## Troubleshooting

### The page returns 404

- Confirm the repository is public.
- Confirm **Settings → Pages → Source** is set to GitHub Actions.
- Open the Actions tab and inspect the `deploy-pages` workflow.
- Confirm the default branch is named `main`.

### The page loads but shows no data

- Confirm the workflow's `Generate demonstration data` step passed.
- Confirm the `Upload Pages artifact` step passed.
- Hard-refresh the browser after a successful redeployment.

### The workflow cannot deploy

Check that the workflow has `pages: write` and `id-token: write` permissions and targets the `github-pages` environment. The included workflow already declares these settings.
