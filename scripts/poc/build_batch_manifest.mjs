// build_batch_manifest.mjs — 공식 마법공식 eligible 집합 기반 배치 manifest 생성 (Phase MF-TTM-OFFICIAL-ELIGIBLE-RECONCILE-AND-BATCH01-PILOT).
//
// 입력: _cache/ttm-poc-output/official-eligible.json (extract_official_eligible.py 가 canonical 함수로 생성)
//   → 근사 eligibility(opMargin>0 등) 사용하지 않는다. 공식 eligible 종목집합을 그대로 사용.
// 정렬: 공식 combinedRank(마법공식 순위) → 결정론. 200종씩 분할.
// 전체 manifest 는 TEMP(_cache, gitignore), 소형 요약 fixture 만 commit.
//
// 실행:  node scripts/poc/build_batch_manifest.mjs   (선행: py scripts/poc/extract_official_eligible.py)
import fs from "node:fs";
import path from "node:path";

const ROOT = path.resolve(process.cwd());
const OFFICIAL = path.join(ROOT, "_cache", "ttm-poc-output", "official-eligible.json");
const CORP = path.join(ROOT, "_cache", "dart-corp-codes.json");
const CFG = path.join(ROOT, "scripts", "poc", "ttm_poc_config.json");
const QCACHE = path.join(ROOT, "_cache", "dart-statements-quarterly");
const OUT_DIR = path.join(ROOT, "_cache", "ttm-poc-output");
const FIXTURE = path.join(ROOT, "scripts", "poc", "batch_manifest_summary.json");

if (!fs.existsSync(OFFICIAL)) {
  console.error("official-eligible.json 없음 — 먼저 `py scripts/poc/extract_official_eligible.py` 실행");
  process.exit(1);
}
const official = JSON.parse(fs.readFileSync(OFFICIAL, "utf8"));
const cfg = JSON.parse(fs.readFileSync(CFG, "utf8"));
const BC = cfg.batchConfig;
const corp = fs.existsSync(CORP) ? JSON.parse(fs.readFileSync(CORP, "utf8")) : {};
const universeBaseDate = official.universeBaseDate || "unknown";

// 공식 eligible = official.codes (이미 combinedRank 순). 결정론 정렬 재확인(rank 오름차순, tie=code).
const eligible = [...official.codes].sort((a, b) =>
  ((a.rank ?? 1e9) - (b.rank ?? 1e9)) || (a.code < b.code ? -1 : 1));

const batchSize = BC.batchSize;
const batchCount = Math.ceil(eligible.length / batchSize);
const pad2 = n => String(n).padStart(2, "0");
const reqReports = [[2026, "11013"], [2025, "11011"], [2025, "11014"], [2025, "11012"], [2025, "11013"]];
function cacheStatusOf(corpCode, fsDiv) {
  if (!corpCode) return "no_corp";
  let hit = 0;
  for (const [y, r] of reqReports) {
    if (fs.existsSync(path.join(QCACHE, `${corpCode}_${y}_${r}_${fsDiv}.json`))) hit++;
  }
  return hit === 0 ? "pending" : (hit === reqReports.length ? "cached" : "partial");
}

const manifestStocks = eligible.map((x, i) => {
  const corpCode = corp[x.code]?.corp_code || "";
  const batchIndex = Math.floor(i / batchSize) + 1;
  return {
    selectionIndex: i,
    batchId: "batch-" + pad2(batchIndex),
    stockCode: x.code,
    corpCode,
    companyName: x.name,
    fsDiv: x.fsDiv,
    industry: x.industry,
    marketCap: x.marketCap,
    officialRank: x.rank,
    expectedReportCount: reqReports.length,
    cacheStatus: cacheStatusOf(corpCode, x.fsDiv),
  };
});

const codes = manifestStocks.map(s => s.stockCode);
const dupCount = codes.length - new Set(codes).size;
const byBatch = {};
manifestStocks.forEach(s => (byBatch[s.batchId] = (byBatch[s.batchId] || 0) + 1));
const cacheAgg = manifestStocks.reduce((m, s) => (m[s.cacheStatus] = (m[s.cacheStatus] || 0) + 1, m), {});

const manifest = {
  schemaVersion: "2.0.0",
  phase: "MF-TTM-OFFICIAL-ELIGIBLE-RECONCILE-AND-BATCH01-PILOT",
  generatedAt: universeBaseDate,
  universeBaseDate,
  eligibleSource: official.source,
  eligibleCount: manifestStocks.length,
  codeSetHash: official.codeSetHash,
  batchSize,
  batchCount,
  integrity: { total: manifestStocks.length, duplicates: dupCount, byBatch },
  cacheStatusAgg: cacheAgg,
  corpCodeMissing: manifestStocks.filter(s => !s.corpCode).length,
  stocks: manifestStocks,
};
if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });
fs.writeFileSync(path.join(OUT_DIR, "ttm-batch-manifest.json"), JSON.stringify(manifest, null, 2));

const sample = {};
for (const bid of Object.keys(byBatch).sort()) {
  sample[bid] = manifestStocks.filter(s => s.batchId === bid).slice(0, 3)
    .map(s => ({ stockCode: s.stockCode, companyName: s.companyName, fsDiv: s.fsDiv, officialRank: s.officialRank, cacheStatus: s.cacheStatus }));
}
fs.writeFileSync(FIXTURE, JSON.stringify({
  schemaVersion: manifest.schemaVersion,
  eligibleSource: official.source,
  universeBaseDate, eligibleCount: manifest.eligibleCount, codeSetHash: official.codeSetHash,
  batchSize, batchCount, integrity: manifest.integrity, cacheStatusAgg: cacheAgg,
  note: "공식 마법공식 eligible(canonical 함수) 기반. 근사 eligibility 미사용. 전체 manifest는 _cache(gitignore), 이 파일은 요약+배치별 샘플 3종만(commit).",
  batchSamples: sample,
}, null, 2));

console.log("eligibleCount:", manifest.eligibleCount, "| codeSetHash:", official.codeSetHash);
console.log("batchSize:", batchSize, "| batchCount:", batchCount);
console.log("duplicates:", dupCount, "| corpCodeMissing:", manifest.corpCodeMissing);
console.log("byBatch:", JSON.stringify(byBatch));
console.log("cacheStatus:", JSON.stringify(cacheAgg));
