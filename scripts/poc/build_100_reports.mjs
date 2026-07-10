// build_100_reports.mjs — 100종 PoC 결과에서 요약/이상치/CSV 산출물 생성 (TEMP, gitignore).
// 입력: _cache/ttm-poc-output/ttm-100-stock-latest.json
// 출력: ttm-100-stock-summary.json / ttm-100-stock-anomalies.json / ttm-100-stock-latest.csv
import fs from "node:fs";
import path from "node:path";

const DIR = path.resolve("_cache/ttm-poc-output");
const j = JSON.parse(fs.readFileSync(path.join(DIR, "ttm-100-stock-latest.json"), "utf8"));
const R = j.results;
const S = j.summary;

const has = v => v != null;
const cov = f => +(R.filter(r => has(r[f])).length / R.length).toFixed(3);
const countBy = (arr, keyfn) => arr.reduce((m, x) => (m[keyfn(x)] = (m[keyfn(x)] || 0) + 1, m), {});

const complete = R.filter(r => has(r.ttmRevenue) && has(r.ttmOperatingIncome) && has(r.ttmNetIncome));
const partial = R.filter(r => (r.warningReasons || []).some(w => w.includes("누락")));
const diff0 = R.filter(r => r.annualReconstruction && r.annualReconstruction.diff === 0);
const reconFalse = R.filter(r => r.annualReconstruction && !r.annualReconstruction.reconstructable);

const extremeFlagRows = R.filter(r => r.gate && (r.gate.incomeExtremeFlags || []).length);
const restateRows = R.filter(r => r.gate && (r.gate.restatementFlags || []).length);
let heukjeok = 0, yoyBig = 0, qGtFy = 0;
R.forEach(r => (r.gate && r.gate.incomeExtremeFlags || []).forEach(f => {
  if (f.includes("흑↔적")) heukjeok++;
  else if (f.includes("YoY")) yoyBig++;
  else if (f.includes("직전연간")) qGtFy++;
}));

const summary = {
  phase: j.summary.phase,
  asOf: j.summary.asOf,
  target: R.length,
  processing: {
    fullyProcessed: complete.length,
    partial: partial.length,
    failed: R.filter(r => r.qualityStatus === "BLOCKED_DATA_INCONSISTENCY").length,
    avgSecPerStock: S.avgSecPerStock,
    totalSecEstimate: +(S.avgSecPerStock * R.length).toFixed(1),
  },
  gateDistribution: S.counts,
  fsDiv: S.fsDivDistribution,
  accountCoverage: {
    ttmRevenue: cov("ttmRevenue"), ttmOperatingIncome: cov("ttmOperatingIncome"), ttmNetIncome: cov("ttmNetIncome"),
    latestCurrentAssets: cov("latestCurrentAssets"), latestCurrentLiabilities: cov("latestCurrentLiabilities"),
    latestPpe: cov("latestPpe"), latestCash: cov("latestCash"), latestTotalDebt: cov("latestTotalDebt"),
  },
  annualReconstruction: {
    diffZero: diff0.length,
    reconstructableFalse_missingReports: reconFalse.length,
    matchRate: +(diff0.length / R.length).toFixed(3),
  },
  anomalies: {
    extremeOutlierStocks: extremeFlagRows.length,
    flagTypes: { blackRedTurnaround: heukjeok, yoySurge: yoyBig, quarterExceedsPriorAnnual: qGtFy },
    restatementStocks: restateRows.length,
    internalConsistencyViolations: R.filter(r => r.gate && r.gate.internalConsistency && !r.gate.internalConsistency.consistent).length,
    missingReportStocks: partial.length,
  },
  dart: S.dart,
  distributionByBand: countBy(R, r => r.marketCapBand || "?"),
  gateByBand: R.reduce((m, r) => {
    const b = r.marketCapBand || "?"; (m[b] ||= {}); m[b][r.qualityStatus] = (m[b][r.qualityStatus] || 0) + 1; return m;
  }, {}),
  fullUniverseProjection_measured: (() => {
    // 실측: 신규 80종(20 캐시) 기준 종목당 콜/시간
    const newStocks = R.length - 20;                        // 캐시된 seed20 제외
    const callsPerNewStock = S.dart.cacheMiss / newStocks;  // 400/80 = 5.0
    const secPerStock = S.avgSecPerStock;
    const targets = [1316, 2365];
    return targets.map(t => ({
      universe: t,
      estDartCalls: Math.round(callsPerNewStock * t),
      estMinutes: +((secPerStock * t) / 60).toFixed(1),
      estHours: +((secPerStock * t) / 3600).toFixed(2),
    })).concat([{ note: "실측 종목당 신규콜≈" + callsPerNewStock.toFixed(1) + ", 초/종≈" + secPerStock + ". DART 일일한도(계정 통상 2만콜)>필요콜 → 하루 내 가능. 재실행은 캐시로 급감." }]);
  })(),
};

fs.writeFileSync(path.join(DIR, "ttm-100-stock-summary.json"), JSON.stringify(summary, null, 2));

// anomalies: WARNING/극단/누락/restatement 종목 상세
const anomalies = R.filter(r =>
  r.qualityStatus !== "PASS" ||
  (r.gate && ((r.gate.incomeExtremeFlags || []).length || (r.gate.restatementFlags || []).length))
).map(r => ({
  stockCode: r.stockCode, companyName: r.companyName, industry: r.industry, marketCapBand: r.marketCapBand,
  fsDiv: r.fsDiv, qualityStatus: r.qualityStatus,
  incomeExtremeFlags: r.gate ? r.gate.incomeExtremeFlags : [],
  restatementFlags: r.gate ? r.gate.restatementFlags : [],
  officialIrMatched: r.gate ? r.gate.officialIrMatch && r.gate.officialIrMatch.matched : false,
  missingReports: (r.warningReasons || []).filter(w => w.includes("누락")),
  ttm: { revenue: r.ttmRevenue, operatingIncome: r.ttmOperatingIncome, netIncome: r.ttmNetIncome },
}));
fs.writeFileSync(path.join(DIR, "ttm-100-stock-anomalies.json"),
  JSON.stringify({ count: anomalies.length, stocks: anomalies }, null, 2));

// CSV: 종목별 핵심 필드
const H = ["stockCode", "companyName", "industry", "marketCapBand", "fsDiv", "qualityStatus",
  "ttmRevenue", "ttmOperatingIncome", "ttmNetIncome", "latestCurrentAssets", "latestCurrentLiabilities",
  "latestPpe", "latestCash", "latestTotalDebt", "annualDiff", "extremeFlags", "missingReports"];
const lines = [H.join(",")];
for (const r of R) {
  const row = [r.stockCode, '"' + (r.companyName || "") + '"', '"' + (r.industry || "") + '"', r.marketCapBand, r.fsDiv,
    r.qualityStatus, r.ttmRevenue, r.ttmOperatingIncome, r.ttmNetIncome,
    r.latestCurrentAssets, r.latestCurrentLiabilities, r.latestPpe, r.latestCash, r.latestTotalDebt,
    r.annualReconstruction ? r.annualReconstruction.diff : "",
    (r.gate ? (r.gate.incomeExtremeFlags || []).length : 0),
    '"' + (r.warningReasons || []).filter(w => w.includes("누락")).join(";") + '"'];
  lines.push(row.map(v => v == null ? "" : v).join(","));
}
fs.writeFileSync(path.join(DIR, "ttm-100-stock-latest.csv"), lines.join("\n") + "\n");

console.log("written: ttm-100-stock-summary.json / anomalies.json / latest.csv");
console.log("gate:", JSON.stringify(summary.gateDistribution));
console.log("gateByBand:", JSON.stringify(summary.gateByBand));
console.log("annualReconstruction:", JSON.stringify(summary.annualReconstruction));
console.log("projection:", JSON.stringify(summary.fullUniverseProjection_measured));
