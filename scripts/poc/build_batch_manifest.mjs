// build_batch_manifest.mjs — TTM 분기수집 대상 결정론 선정 + 200종 배치 분할 (Phase MF-TTM-ELIGIBLE-BATCH-RUNNER-PREP).
//
// 무작위 없음. 동일 입력 → 동일 대상·순서·배치. financial-universe-real.json 은 read-only.
// 전체 manifest 는 TEMP(_cache, gitignore)에 쓰고, 소형 요약 fixture 만 commit 한다.
//
// 실행: node scripts/poc/build_batch_manifest.mjs
import fs from "node:fs";
import path from "node:path";

const ROOT = path.resolve(process.cwd());
const UNI = path.join(ROOT, "financial-universe-real.json");
const CORP = path.join(ROOT, "_cache", "dart-corp-codes.json");
const CFG = path.join(ROOT, "scripts", "poc", "ttm_poc_config.json");
const QCACHE = path.join(ROOT, "_cache", "dart-statements-quarterly");
const OUT_DIR = path.join(ROOT, "_cache", "ttm-poc-output");
const FIXTURE = path.join(ROOT, "scripts", "poc", "batch_manifest_summary.json");

const cfg = JSON.parse(fs.readFileSync(CFG, "utf8"));
const EF = cfg.eligibilityFilter;
const BC = cfg.batchConfig;
const uni = JSON.parse(fs.readFileSync(UNI, "utf8"));
const universeBaseDate = uni.meta?.baseDate || "unknown";
const rows = uni.data;
const corp = fs.existsSync(CORP) ? JSON.parse(fs.readFileSync(CORP, "utf8")) : {};
const excludeRe = new RegExp(EF.excludeIndustryPattern);

// 1) 결정론 eligible 필터
const eligible = rows.filter(x =>
  x.dartLatestYear === EF.dartLatestYear &&
  x.marketCap != null && x.marketCap >= EF.minMarketCapBillionKrw &&
  (x.dartFsDiv === "CFS" || x.dartFsDiv === "OFS") &&
  (!EF.requirePositiveOpMargin || (x.opMargin != null && x.opMargin > 0)) &&
  !excludeRe.test(x.industryName || "")
);

// 2) 결정론 정렬: marketCap 내림차순, tie=code 오름차순
eligible.sort((a, b) => (b.marketCap - a.marketCap) || (a.symbol < b.symbol ? -1 : 1));

// 3) 배치 분할(batchSize)
const batchSize = BC.batchSize;
const batchCount = Math.ceil(eligible.length / batchSize);
const pad2 = n => String(n).padStart(2, "0");

// 분기 캐시 존재 여부(cacheStatus) — reprt 5종 중 몇 개 존재
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
  const corpCode = corp[x.symbol]?.corp_code || "";
  const batchIndex = Math.floor(i / batchSize) + 1;
  return {
    selectionIndex: i,
    batchId: "batch-" + pad2(batchIndex),
    stockCode: x.symbol,
    corpCode,
    companyName: x.corpName,
    fsDiv: x.dartFsDiv,
    industry: x.industryName || "(공백)",
    marketCap: x.marketCap,
    expectedReportCount: reqReports.length,
    cacheStatus: cacheStatusOf(corpCode, x.dartFsDiv),
  };
});

// 무결성: 누락 0 / 중복 0 / 배치별 개수
const codes = manifestStocks.map(s => s.stockCode);
const dupCount = codes.length - new Set(codes).size;
const byBatch = {};
manifestStocks.forEach(s => (byBatch[s.batchId] = (byBatch[s.batchId] || 0) + 1));
const cacheAgg = manifestStocks.reduce((m, s) => (m[s.cacheStatus] = (m[s.cacheStatus] || 0) + 1, m), {});

const manifest = {
  schemaVersion: "1.0.0",
  phase: "MF-TTM-ELIGIBLE-BATCH-RUNNER-PREP",
  generatedAt: universeBaseDate,          // 결정론: universe baseDate 사용(Date.now 미사용)
  universeBaseDate,
  eligibilityFilter: EF,
  eligibleCount: manifestStocks.length,
  batchSize,
  batchCount,
  integrity: { total: manifestStocks.length, duplicates: dupCount, byBatch },
  cacheStatusAgg: cacheAgg,
  corpCodeMissing: manifestStocks.filter(s => !s.corpCode).length,
  stocks: manifestStocks,
};
if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });
fs.writeFileSync(path.join(OUT_DIR, "ttm-batch-manifest.json"), JSON.stringify(manifest, null, 2));

// 소형 fixture(commit 대상): 요약 + 각 배치 앞 3종 샘플만
const sample = {};
for (const bid of Object.keys(byBatch).sort()) {
  sample[bid] = manifestStocks.filter(s => s.batchId === bid).slice(0, 3)
    .map(s => ({ stockCode: s.stockCode, companyName: s.companyName, fsDiv: s.fsDiv, cacheStatus: s.cacheStatus }));
}
fs.writeFileSync(FIXTURE, JSON.stringify({
  schemaVersion: manifest.schemaVersion,
  universeBaseDate, eligibleCount: manifest.eligibleCount, batchSize, batchCount,
  integrity: manifest.integrity, cacheStatusAgg: cacheAgg,
  note: "전체 manifest 는 _cache(gitignore)에 생성. 이 파일은 소형 요약+배치별 샘플 3종만(commit).",
  batchSamples: sample,
}, null, 2));

console.log("eligibleCount:", manifest.eligibleCount, "| batchSize:", batchSize, "| batchCount:", batchCount);
console.log("duplicates:", dupCount, "| corpCodeMissing:", manifest.corpCodeMissing);
console.log("byBatch:", JSON.stringify(byBatch));
console.log("cacheStatus:", JSON.stringify(cacheAgg));
console.log("full manifest → _cache/ttm-poc-output/ttm-batch-manifest.json (gitignore)");
console.log("fixture →", FIXTURE);
