// select_ttm_100.mjs — eligible universe에서 결정론적으로 100종목 선정 (Phase MF-TTM-DART-100-STOCK-SCALE-CHECK).
//
// 무작위 없음(Math.random 미사용). 동일 universe 입력 → 동일 100종.
// 선정: 기존 20종(seed) 강제 포함 + 업종/시총밴드 라운드로빈 분산 + OFS 최소 보장.
// 출력: scripts/poc/ttm_100_config.json (quarterly_ttm_poc.py 가 POC_CONFIG 로 읽음).
//
// 실행: node scripts/poc/select_ttm_100.mjs
import fs from "node:fs";
import path from "node:path";

const ROOT = path.resolve(process.cwd());
const UNI = path.join(ROOT, "financial-universe-real.json");
const RANK = path.join(ROOT, "magic-formula-rankings.json");
const BASE_CFG = path.join(ROOT, "scripts", "poc", "ttm_poc_config.json");
const OUT = path.join(ROOT, "scripts", "poc", "ttm_100_config.json");

const EXCLUDE_INDUSTRY = /금융|기타금융|증권|보험|은행|부동산|전기·가스|가스·수도|·수도/;
const TARGET = 100;
const MIN_OFS = 18;

const uni = JSON.parse(fs.readFileSync(UNI, "utf8")).data;
const rank = JSON.parse(fs.readFileSync(RANK, "utf8"));
const baseCfg = JSON.parse(fs.readFileSync(BASE_CFG, "utf8"));
const seed = baseCfg.stocks;                       // 기존 20종
const seedCodes = new Set(seed.map(s => s.code));
const magicRankByCode = Object.fromEntries((rank.top100 || []).map(x => [x.code, x.rank]));

// 1) eligible 필터: 2025 결산 + 금융/유틸 제외 + marketCap 존재 + fsDiv 유효
const eligible = uni.filter(x =>
  x.dartLatestYear === 2025 &&
  x.marketCap != null && x.marketCap > 0 &&
  (x.dartFsDiv === "CFS" || x.dartFsDiv === "OFS") &&
  !EXCLUDE_INDUSTRY.test(x.industryName || "")
);

// 2) 시총 밴드(3분위) — marketCap 내림차순, 인덱스 3등분
const byCap = [...eligible].sort((a, b) => (b.marketCap - a.marketCap) || (a.symbol < b.symbol ? -1 : 1));
const n = byCap.length;
const t1 = Math.floor(n / 3), t2 = Math.floor((2 * n) / 3);
const bandOf = {};
byCap.forEach((x, i) => { bandOf[x.symbol] = i < t1 ? "large" : (i < t2 ? "mid" : "small"); });

// 3) selected = seed 20
const selected = new Map();                          // code -> record
function rec(x, reason) {
  return {
    code: x.symbol, name: x.corpName, fsDiv: x.dartFsDiv, industry: x.industryName || "(공백)",
    marketCap: x.marketCap, marketCapBand: bandOf[x.symbol],
    magicRank: magicRankByCode[x.symbol] ?? null,
    opIncomeGrowth: x.opIncomeGrowth ?? null,
    selectionReason: reason,
  };
}
const uniByCode = Object.fromEntries(uni.map(x => [x.symbol, x]));
for (const s of seed) {
  const x = uniByCode[s.code];
  if (x) selected.set(s.code, rec(x, "seed20"));
}

// 4) 후보: eligible - seed, (band, industry) 그룹, 업종 내 marketCap 내림차순
const pool = byCap.filter(x => !seedCodes.has(x.symbol));
const bands = ["large", "mid", "small"];
const byBandIndustry = { large: {}, mid: {}, small: {} };
for (const x of pool) {
  const b = bandOf[x.symbol], ind = x.industryName || "(공백)";
  (byBandIndustry[b][ind] ||= []).push(x);
}
// 각 업종 리스트는 marketCap 내림차순(byCap 순서 유지됨). 업종명은 정렬해 결정론 순회.
const industriesByBand = {};
for (const b of bands) industriesByBand[b] = Object.keys(byBandIndustry[b]).sort();

// 5) 라운드로빈: band 순환 × 업종 순환 × 업종 내 상위부터
const cursor = { large: 0, mid: 0, small: 0 };       // 업종 인덱스 커서
const idxInIndustry = {};                            // "band|ind" -> next idx
let bandTurn = 0;
function pickOne() {
  for (let tries = 0; tries < bands.length; tries++) {
    const b = bands[(bandTurn + tries) % bands.length];
    const inds = industriesByBand[b];
    if (!inds.length) continue;
    for (let k = 0; k < inds.length; k++) {
      const ind = inds[(cursor[b] + k) % inds.length];
      const key = b + "|" + ind;
      const list = byBandIndustry[b][ind];
      let i = idxInIndustry[key] || 0;
      while (i < list.length && selected.has(list[i].symbol)) i++;
      idxInIndustry[key] = i + 1;
      if (i < list.length) {
        cursor[b] = (cursor[b] + k + 1) % inds.length;
        bandTurn = (bands.indexOf(b) + 1) % bands.length;
        return list[i];
      }
    }
  }
  return null;
}
while (selected.size < TARGET) {
  const x = pickOne();
  if (!x) break;
  selected.set(x.symbol, rec(x, `band=${bandOf[x.symbol]}/industry=${x.industryName}/rr`));
}

// 6) OFS 최소 보장(결정론): 부족하면 OFS 후보(marketCap 내림차순) 추가 + band=small CFS(비-seed) 최소부터 제거
function countOFS() { return [...selected.values()].filter(r => r.fsDiv === "OFS").length; }
if (countOFS() < MIN_OFS) {
  const ofsCand = pool.filter(x => x.dartFsDiv === "OFS" && !selected.has(x.symbol)); // 이미 marketCap 내림차순
  for (const x of ofsCand) {
    if (countOFS() >= MIN_OFS) break;
    // 제거 대상: seed 아님 & CFS & band=small, marketCap 최소부터
    const removable = [...selected.values()]
      .filter(r => r.selectionReason !== "seed20" && r.fsDiv === "CFS" && r.marketCapBand === "small")
      .sort((a, b) => a.marketCap - b.marketCap);
    if (!removable.length) break;
    selected.delete(removable[0].code);
    selected.set(x.symbol, rec(x, `OFS-quota/band=${bandOf[x.symbol]}/industry=${x.industryName}`));
  }
}

// 7) 정확히 100 정렬(코드 오름차순)
const stocks = [...selected.values()].sort((a, b) => (a.code < b.code ? -1 : 1)).slice(0, TARGET);

// 8) 집계
const summ = (key) => stocks.reduce((m, s) => (m[s[key]] = (m[s[key]] || 0) + 1, m), {});
const selectionSummary = {
  total: stocks.length,
  seed20Included: stocks.filter(s => seedCodes.has(s.code)).length,
  top100Included: stocks.filter(s => s.magicRank != null).length,
  byFsDiv: summ("fsDiv"),
  byBand: summ("marketCapBand"),
  byIndustry: summ("industry"),
  turnaroundOrLossSample: stocks.filter(s => s.opIncomeGrowth != null && s.opIncomeGrowth < 0).length,
};

const out = {
  phase: "MF-TTM-DART-100-STOCK-SCALE-CHECK",
  purpose: "eligible universe에서 결정론적으로 선정한 100종목 규모 검증(수집·복원·게이트·통계). 운영 반영 없음.",
  generatedFrom: { universe: "financial-universe-real.json", rankings: "magic-formula-rankings.json (top100)" },
  selectionCriteria: "2025결산 + 금융/유틸/부동산 제외 + marketCap>0. seed20 강제포함 + (시총밴드×업종) 라운드로빈 분산 + OFS>=18 보장. 무작위 없음(결정론).",
  eligibleCountUsed: eligible.length,
  ttmTarget: baseCfg.ttmTarget,
  fsDivPolicy: baseCfg.fsDivPolicy,
  officialIrConfirmations: baseCfg.officialIrConfirmations,   // 기존 삼성/SK 공식 IR 확인 데이터 승계
  officialIrConfirmationsNote: baseCfg.officialIrConfirmationsNote,
  selectionSummary,
  stocks,
};
fs.writeFileSync(OUT, JSON.stringify(out, null, 2), "utf8");
console.log("written:", OUT);
console.log("total:", stocks.length, "| seed20:", selectionSummary.seed20Included, "| top100:", selectionSummary.top100Included);
console.log("fsDiv:", JSON.stringify(selectionSummary.byFsDiv), "| band:", JSON.stringify(selectionSummary.byBand));
console.log("industries:", Object.keys(selectionSummary.byIndustry).length, JSON.stringify(selectionSummary.byIndustry));
console.log("eligible pool:", eligible.length);
