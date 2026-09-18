async function jsonl(path) {
  const response = await fetch(path, { cache: "no-store" });
  if (!response.ok) throw new Error(`${path}: HTTP ${response.status}`);
  const body = await response.text();
  return body.trim() ? body.trim().split("\n").map(JSON.parse) : [];
}

// Locally the dashboard is served from /dashboard/ while GitHub Pages publishes
// the dashboard at the site root with generated data under /data/.
const DATA_ROOT = window.location.pathname.includes("/dashboard/") ? "../build/demo" : "./data";

function metricLabel(key) {
  return {
    validity_rate_gte_99_percent: "Valid records ≥ 99%",
    duplicate_rate_lte_1_percent: "Duplicate records ≤ 1%",
    freshness_lte_300_seconds: "Data freshness ≤ 5 minutes",
  }[key] || key;
}

function renderBars(rows) {
  const target = document.querySelector("#gold-bars");
  if (!rows.length) {
    target.innerHTML = '<p class="note">No Gold rows were produced.</p>';
    return;
  }
  const max = Math.max(...rows.map(row => Number(row.gmv)), 1);
  target.innerHTML = rows.map(row => `
    <div class="bar-row">
      <div class="bar-label"><span>${row.status} · ${row.currency}</span><strong>${row.gmv}</strong></div>
      <div class="bar-track"><div class="bar-fill" style="width:${Math.max(5, Number(row.gmv) / max * 100)}%"></div></div>
    </div>`).join("");
}

async function load() {
  try {
    const [reportResponse, gold, quarantine] = await Promise.all([
      fetch(`${DATA_ROOT}/reports/pipeline_report.json`, { cache: "no-store" }),
      jsonl(`${DATA_ROOT}/gold/orders_by_minute.jsonl`),
      jsonl(`${DATA_ROOT}/quarantine/rejected_events.jsonl`),
    ]);
    if (!reportResponse.ok) throw new Error(`report: HTTP ${reportResponse.status}`);
    const report = await reportResponse.json();

    document.querySelector("#health").textContent = report.healthy ? "HEALTHY" : "DEGRADED (EXPECTED)";
    document.querySelector(".pulse").classList.add(report.healthy ? "good" : "bad");
    document.querySelector("#bronze-count").textContent = report.records.bronze;
    document.querySelector("#silver-count").textContent = report.records.silver;
    document.querySelector("#gold-count").textContent = report.records.gold;
    document.querySelector("#quarantine-count").textContent = report.records.quarantine;
    document.querySelector("#generated-at").textContent = report.generated_at;

    document.querySelector("#slo-list").innerHTML = Object.entries(report.slos).map(([key, passed]) => `
      <div class="slo">
        <strong>${metricLabel(key)}</strong>
        <span class="${passed ? "pass" : "fail"}">${passed ? "PASS" : "FAIL"}</span>
      </div>`).join("");

    renderBars(gold);
    document.querySelector("#quarantine-table").innerHTML = quarantine.map(row => `
      <tr>
        <td>${row.event.event_id || "unknown"}</td>
        <td>${row.event.order_id || "unknown"}</td>
        <td>${row.reason}</td>
      </tr>`).join("");
  } catch (error) {
    document.querySelector("#health").textContent = "DATA NOT GENERATED";
    document.querySelector(".pulse").classList.add("bad");
    console.error(error);
  }
}

load();
