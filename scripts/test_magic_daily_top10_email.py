# test_magic_daily_top10_email.py
# 마법공식 일일 top10 이메일 미리보기(MF-DAILY-TOP10-EMAIL-PIPELINE-ROOT-CAUSE-AND-RECOVERY) 회귀 테스트.
#   목적: 이메일 제목·본문 렌더링, 전일 대비 diff, 발송 멱등성(is_already_sent/mark_sent),
#         비거래일/신호없음/유니버스stale/top10불완전 BLOCKED 분기를 fixture로 고정한다.
#   방식: 전부 임시 디렉터리(tempfile)에서 동작. 실제 TEMP(%LOCALAPPDATA%)·운영 파일 접근 0.
#         실주문·브로커 API·SMTP 호출 0(코드 자체에 존재하지 않음).
# 사용: python scripts\test_magic_daily_top10_email.py

import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import magic_daily_common as C  # noqa: E402
import magic_daily_top10_email as E  # noqa: E402

_pass = 0
_fail = 0


def check(name, actual, expected):
    global _pass, _fail
    if actual == expected:
        _pass += 1
        print(f"  PASS  {name}")
    else:
        _fail += 1
        print(f"  FAIL  {name}  expected={expected!r} actual={actual!r}")


def make_ranking(date_iso, codes_ranks):
    """codes_ranks: [(rank, code, name), ...] -> rankings.json 형태 dict."""
    top10 = []
    for rank, code, name in codes_ranks:
        top10.append({
            "rank": rank, "code": code, "name": name,
            "valueRank": rank, "profitabilityRank": rank * 2, "combinedRank": rank * 3,
            "signalClosePrice": 10000 + rank,
        })
    return {
        "signalAsOfDate": date_iso, "universeBaseDate": date_iso,
        "formulaVersion": "book-faithful-v1-2026-43B5", "eligibleCount": 1316,
        "top10": top10,
    }


TODAY_CODES = [(1, "046940", "우원개발"), (2, "461300", "아이스크림미디어"), (3, "088130", "동아엘텍"),
               (4, "124500", "아이티센글로벌"), (5, "184230", "SGA솔루션즈"), (6, "171090", "선익시스템"),
               (7, "053580", "웹케시"), (8, "052400", "코나아이"), (9, "018290", "브이티"),
               (10, "484870", "엠앤씨솔루션")]

PREV_CODES = [(1, "046940", "우원개발"), (2, "461300", "아이스크림미디어"), (3, "088130", "동아엘텍"),
              (4, "124500", "아이티센글로벌"), (5, "171090", "선익시스템"), (6, "184230", "SGA솔루션즈"),
              (7, "053580", "웹케시"), (8, "052400", "코나아이"), (9, "018290", "브이티"),
              (10, "999999", "제외예정종목")]


print("=== 마법공식 일일 top10 이메일 미리보기 회귀 테스트 (fixture 전용, 운영/실 TEMP 미접근) ===")

# ── compute_diff ──────────────────────────────────────────────────────────
today_ranking = make_ranking("2026-07-24", TODAY_CODES)
prev_ranking = make_ranking("2026-07-23", PREV_CODES)
diff = E.compute_diff(today_ranking["top10"], prev_ranking["top10"])
print("[1] compute_diff — 순위 변화/신규/제외")
check("comparisonAvailable", diff["comparisonAvailable"], True)
row5 = next(r for r in diff["rows"] if r["code"] == "184230")
check("184230 rankChange(6->5, 상승1)", row5["rankChange"], 1)
check("184230 isNew", row5["isNew"], False)
row9 = next(r for r in diff["rows"] if r["code"] == "484870")
check("484870 isNew(신규 진입)", row9["isNew"], True)
check("removed 1건(999999)", [r["code"] for r in diff["removed"]], ["999999"])

print("[2] compute_diff — 전일 데이터 없음(비교불가)")
diff_none = E.compute_diff(today_ranking["top10"], None)
check("comparisonAvailable=False", diff_none["comparisonAvailable"], False)
check("rows 전부 isNew=None", all(r["isNew"] is None for r in diff_none["rows"]), True)

# ── render_email ──────────────────────────────────────────────────────────
print("[3] render_email — 제목/본문 필수 항목")
email = E.render_email("2026-07-24", today_ranking, diff)
check("제목 형식", email["subject"], "[와바바] 2026-07-24 마법공식 매수 검토 10종목")
body = email["body"]
for must in ("[프로젝트] 와바바", "[기준 거래일] 2026-07-24", "[전체 판정] PASS", "[대장이 할 일]",
             "[실주문] 0", "046940", "종합점수", "투자 추천이 아닙니다"):
    check(f"본문 포함: {must}", must in body, True)
check("본문에 제외 종목 표기", "999999" in body or "제외예정종목" in body or "제외된 종목" in body, True)

# ── is_already_sent / mark_sent (격리된 임시 상태파일) ──────────────────────
print("[4] 발송 멱등성(is_already_sent/mark_sent, 임시 상태파일)")
with tempfile.TemporaryDirectory() as tmp:
    state_path = Path(tmp) / "delivery-state.json"
    check("최초엔 미발송", E.is_already_sent("2026-07-24", state_path=state_path), False)
    E.mark_sent("2026-07-24", state_path=state_path, now="2026-07-24T16:20:00+09:00")
    check("mark_sent 후 발송기록 있음", E.is_already_sent("2026-07-24", state_path=state_path), True)
    check("다른 날짜는 영향 없음", E.is_already_sent("2026-07-25", state_path=state_path), False)
    state_data = json.loads(state_path.read_text(encoding="utf-8"))
    check("상태파일에 개인정보 없음(날짜/시각만)",
          set(state_data.get("sentDates", {}).get("2026-07-24", "")[:4]).issubset(set("0123456789")), True)

# ── run_top10_email — 전체 분기(임시 TEMP_ROOT로 격리) ───────────────────────
print("[5] run_top10_email — 분기별 상태(임시 디렉터리, 실 TEMP 미접근)")
_orig_temp_root = C.TEMP_ROOT
try:
    with tempfile.TemporaryDirectory() as tmp:
        C.TEMP_ROOT = Path(tmp)  # magic_daily_top10_email.C 는 같은 모듈 객체를 참조하므로 함께 반영됨
        state_path = C.TEMP_ROOT / "reports" / "top10-email-delivery-state.json"

        # 5-1) 비거래일(토요일) self-skip
        r = E.run_top10_email("2026-07-25", state_path=state_path)  # 2026-07-25 는 토요일
        check("토요일 self-skip", r["status"], "SELF_SKIPPED_NON_TRADING_DAY")
        check("self-skip emailSent=False", r["emailSent"], False)

        # 5-2) 신호 패키지 없음 -> BLOCKED_SIGNAL_NOT_READY
        r = E.run_top10_email("2026-07-24", state_path=state_path)
        check("신호 없음 -> BLOCKED_SIGNAL_NOT_READY", r["blockedCode"], E.BLOCKED_SIGNAL_NOT_READY)
        check("BLOCKED에서도 productionWriteCount=0", r["productionWriteCount"], 0)

        # 5-3) 신호 패키지 생성(오늘) + 정상 케이스 -> DRY_RUN_EMAIL_READY
        pkg_dir = C.TEMP_ROOT / "2026-07-24"
        pkg_dir.mkdir(parents=True, exist_ok=True)
        (pkg_dir / "rankings.json").write_text(json.dumps(today_ranking, ensure_ascii=False), encoding="utf-8")
        prev_dir = C.TEMP_ROOT / "2026-07-23"
        prev_dir.mkdir(parents=True, exist_ok=True)
        (prev_dir / "rankings.json").write_text(json.dumps(prev_ranking, ensure_ascii=False), encoding="utf-8")

        r = E.run_top10_email("2026-07-24", state_path=state_path)
        check("정상 -> DRY_RUN_EMAIL_READY", r["status"], E.DRY_RUN_EMAIL_READY)
        check("emailSent 항상 False(이 Phase는 미발송)", r["emailSent"], False)
        check("smtpCallCount=0", r["smtpCallCount"], 0)
        check("realOrderCount=0", r["realOrderCount"], 0)
        check("brokerApiCallCount=0", r["brokerApiCallCount"], 0)
        check("prevTradingDate=2026-07-23", r["prevTradingDate"], "2026-07-23")
        check("newEntryCount=1(484870)", r["newEntryCount"], 1)
        check("removedCount=1(999999)", r["removedCount"], 1)
        check("recipientConfigured=False(값 미참조)", r["recipientConfigured"], False)

        # 5-4) 이미 발송된 날짜 -> ALREADY_SENT(중복 방지)
        E.mark_sent("2026-07-24", state_path=state_path)
        r = E.run_top10_email("2026-07-24", state_path=state_path)
        check("mark_sent 후 재실행 -> ALREADY_SENT", r["status"], E.ALREADY_SENT)
        check("ALREADY_SENT emailSent=False", r["emailSent"], False)

        # 5-5) 유니버스/시세 stale(신호 패키지의 signalAsOfDate 가 요청일과 다름) -> BLOCKED_UNIVERSE_STALE
        stale_dir = C.TEMP_ROOT / "2026-07-27"
        stale_dir.mkdir(parents=True, exist_ok=True)
        stale_ranking = make_ranking("2026-07-24", TODAY_CODES)  # 24일자 시세를 27일 신호로 오기
        (stale_dir / "rankings.json").write_text(json.dumps(stale_ranking, ensure_ascii=False), encoding="utf-8")
        r = E.run_top10_email("2026-07-27", state_path=state_path)
        check("stale 시세 -> BLOCKED_UNIVERSE_STALE", r["blockedCode"], E.BLOCKED_UNIVERSE_STALE)

        # 5-6) top10 불완전(9종목) -> BLOCKED_TOP10_INCOMPLETE
        incomplete_dir = C.TEMP_ROOT / "2026-07-28"
        incomplete_dir.mkdir(parents=True, exist_ok=True)
        incomplete_ranking = make_ranking("2026-07-28", TODAY_CODES[:9])
        (incomplete_dir / "rankings.json").write_text(
            json.dumps(incomplete_ranking, ensure_ascii=False), encoding="utf-8")
        r = E.run_top10_email("2026-07-28", state_path=state_path)
        check("9종목 -> BLOCKED_TOP10_INCOMPLETE", r["blockedCode"], E.BLOCKED_TOP10_INCOMPLETE)
finally:
    C.TEMP_ROOT = _orig_temp_root

print("")
print(f"결과: PASS {_pass} / FAIL {_fail}")
print("verdict: " + ("PASS" if _fail == 0 else "FAIL"))
sys.exit(1 if _fail else 0)
