import json
from pathlib import Path
from datetime import datetime


ROOT_DIR = Path(__file__).resolve().parents[1]

MARKET_SNAPSHOT_PATH = ROOT_DIR / "financial-universe-real.json"
COMPANY_PROFILES_PATH = ROOT_DIR / "company-profiles.json"


MANUAL_PROFILES = {
    "005930": "메모리 반도체·스마트폰 글로벌 대표 기업",
    "000660": "메모리 반도체 글로벌 상위 기업",
    "005380": "완성차 글로벌 대표 제조사",
    "000270": "완성차·전기차 글로벌 제조사",
    "015760": "국내 전력 판매·송배전망 중심 공기업",
    "329180": "초등 디지털 교육 콘텐츠·교과서 플랫폼 기업",
    "461300": "초등 디지털 교육 콘텐츠·교과서 플랫폼 기업",
    "009410": "토목·건축·환경 인프라 종합건설 기업",
    "187790": "SCR 탈질촉매 기반 대기환경 소재 기업",
    "021080": "성장기업 투자 중심 벤처캐피탈 기업",
    "015360": "도시가스·에너지 인프라 기반 투자 지주형 기업",
}


def now_text():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def read_json(path):
    if not path.exists():
        return None

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path, data):
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_items(data):
    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        for key in ["items", "universe", "data", "stocks", "recommendations"]:
            value = data.get(key)
            if isinstance(value, list):
                return value

    return []


def clean_text(value):
    return " ".join(str(value or "").replace("\n", " ").replace("\r", " ").split()).strip()


def get_code(item):
    return clean_text(
        item.get("code")
        or item.get("stockCode")
        or item.get("ticker")
        or item.get("symbol")
        or item.get("isin")
        or ""
    )


def get_name(item):
    return clean_text(
        item.get("name")
        or item.get("stockName")
        or item.get("companyName")
        or item.get("corpName")
        or get_code(item)
    )


def get_first_text(item, keys):
    for key in keys:
        value = item.get(key)
        if isinstance(value, str) and value.strip():
            return clean_text(value)
    return ""


def build_source_text(item):
    pieces = [
        get_name(item),
        get_first_text(
            item,
            [
                "industry",
                "sector",
                "market",
                "mainBusiness",
                "businessSummary",
                "companySummary",
                "description",
                "corpOverview",
                "productSummary",
                "theme",
                "themesText",
            ],
        ),
    ]

    themes = item.get("themes")
    if isinstance(themes, list):
        pieces.extend([clean_text(value) for value in themes if clean_text(value)])

    news = item.get("news")
    if isinstance(news, dict):
        for key in ["title", "headline", "summary", "description"]:
            value = news.get(key)
            if isinstance(value, str) and value.strip():
                pieces.append(clean_text(value))

        news_themes = news.get("themes")
        if isinstance(news_themes, list):
            pieces.extend([clean_text(value) for value in news_themes if clean_text(value)])

    return " ".join([piece for piece in pieces if piece])


def contains_any(text, keywords):
    return any(keyword in text for keyword in keywords)


def infer_profile_by_name(name, source):
    joined = f"{name} {source}"

    if contains_any(joined, ["삼성전자"]):
        return "메모리 반도체·스마트폰 글로벌 대표 기업"
    if contains_any(joined, ["SK하이닉스", "하이닉스"]):
        return "메모리 반도체 글로벌 상위 기업"
    if contains_any(joined, ["현대차", "현대자동차"]):
        return "완성차·전기차 글로벌 제조사"
    if contains_any(joined, ["기아"]):
        return "완성차·전기차 글로벌 제조사"
    if contains_any(joined, ["한국전력", "한전"]):
        return "국내 전력 판매·송배전망 중심 공기업"
    if contains_any(joined, ["NAVER", "네이버"]):
        return "검색·커머스·콘텐츠 중심 국내 대표 인터넷 플랫폼"
    if contains_any(joined, ["카카오"]):
        return "모바일 메신저·콘텐츠 중심 플랫폼 기업"
    if contains_any(joined, ["현대중공업", "HD현대중공업"]):
        return "대형 선박 건조 중심 글로벌 조선사"
    if contains_any(joined, ["삼성중공업"]):
        return "LNG선·해양플랜트 중심 글로벌 조선사"
    if contains_any(joined, ["한화오션", "대우조선"]):
        return "특수선·LNG선 중심 글로벌 조선사"
    if contains_any(joined, ["POSCO", "포스코"]):
        return "철강·소재 중심 국내 대표 철강 기업"
    if contains_any(joined, ["LG전자"]):
        return "가전·전장 중심 글로벌 전자 기업"
    if contains_any(joined, ["LG화학"]):
        return "석유화학·첨단소재 중심 종합 화학 기업"
    if contains_any(joined, ["삼성바이오로직스"]):
        return "바이오의약품 위탁개발·생산 글로벌 기업"
    if contains_any(joined, ["셀트리온"]):
        return "바이오시밀러 중심 바이오의약품 기업"

    return ""


def infer_profile_by_keywords(item):
    name = get_name(item)
    source = build_source_text(item)
    joined = f"{name} {source}"

    direct = infer_profile_by_name(name, source)
    if direct:
        return direct

    if contains_any(joined, ["은행", "금융지주", "KB금융", "신한지주", "하나금융", "우리금융", "JB금융", "BNK금융", "DGB금융"]):
        return "예대마진·비이자이익 기반 금융지주·은행 기업"

    if contains_any(joined, ["증권", "투자증권", "키움", "미래에셋증권", "NH투자"]):
        return "브로커리지·IB·자산관리 중심 증권사"

    if contains_any(joined, ["보험", "생명", "화재", "손해보험"]):
        return "보험료 수입과 운용자산 기반 보험사"

    if contains_any(joined, ["카드", "캐피탈", "리스"]):
        return "여신금융·할부금융 중심 금융 기업"

    if contains_any(joined, ["반도체", "메모리", "파운드리", "팹리스", "웨이퍼", "소자"]):
        return "반도체 밸류체인 핵심 기업"

    if contains_any(joined, ["PCB", "기판", "패키징", "후공정", "검사장비", "반도체 장비", "노광", "식각", "증착"]):
        return "반도체 장비·부품·소재 공급 기업"

    if contains_any(joined, ["디스플레이", "OLED", "LCD", "패널"]):
        return "디스플레이 소재·장비·부품 기업"

    if contains_any(joined, ["2차전지", "이차전지", "배터리", "양극재", "음극재", "전해액", "분리막"]):
        return "2차전지 소재·부품 밸류체인 기업"

    if contains_any(joined, ["전기차", "EV", "전장", "자율주행", "모빌리티"]):
        return "전기차·전장 부품 밸류체인 기업"

    if contains_any(joined, ["자동차", "완성차", "차량", "부품", "모듈"]):
        return "자동차 부품·모듈 공급 기업"

    if contains_any(joined, ["조선", "선박", "LNG선", "해양플랜트", "선박엔진"]):
        return "조선·선박 기자재 밸류체인 기업"

    if contains_any(joined, ["해운", "운송", "물류", "컨테이너", "항만"]):
        return "해운·물류 운송 서비스 기업"

    if contains_any(joined, ["항공", "공항", "여행", "면세"]):
        return "항공·여행 수요 회복 관련 기업"

    if contains_any(joined, ["건설", "토목", "건축", "분양", "부동산", "SOC"]):
        return "토목·건축·개발 중심 종합건설 기업"

    if contains_any(joined, ["시멘트", "레미콘", "골재"]):
        return "건설자재·시멘트 공급 기업"

    if contains_any(joined, ["철강", "강관", "스테인리스", "압연", "후판"]):
        return "철강 제품 제조·가공 기업"

    if contains_any(joined, ["화학", "석유화학", "합성수지", "첨단소재", "소재"]):
        return "화학·첨단소재 제조 기업"

    if contains_any(joined, ["정유", "석유", "윤활유", "가스", "LPG", "도시가스"]):
        return "에너지·가스 공급 기반 기업"

    if contains_any(joined, ["전력", "송전", "변압", "발전", "원전", "태양광", "풍력", "수소"]):
        return "전력·에너지 인프라 관련 기업"

    if contains_any(joined, ["제약", "바이오", "의약품", "신약", "바이오시밀러"]):
        return "의약품·바이오 파이프라인 중심 기업"

    if contains_any(joined, ["의료기기", "진단", "임플란트", "치과", "헬스케어"]):
        return "의료기기·헬스케어 제품 기업"

    if contains_any(joined, ["화장품", "뷰티", "ODM", "OEM", "미용"]):
        return "화장품·뷰티 제품 제조·브랜드 기업"

    if contains_any(joined, ["식품", "음료", "라면", "제과", "외식", "프랜차이즈"]):
        return "식품·음료 소비재 기업"

    if contains_any(joined, ["의류", "패션", "섬유", "브랜드", "스포츠웨어"]):
        return "패션·의류 브랜드 및 제조 기업"

    if contains_any(joined, ["게임", "콘텐츠", "엔터", "음악", "드라마", "영화", "웹툰"]):
        return "게임·콘텐츠 IP 기반 기업"

    if contains_any(joined, ["교육", "교과서", "에듀테크", "학원", "강의"]):
        return "교육 콘텐츠·에듀테크 플랫폼 기업"

    if contains_any(joined, ["소프트웨어", "클라우드", "AI", "데이터", "보안", "솔루션", "SI"]):
        return "소프트웨어·데이터 솔루션 기업"

    if contains_any(joined, ["통신", "5G", "네트워크", "인터넷", "IDC", "데이터센터"]):
        return "통신·데이터 인프라 기업"

    if contains_any(joined, ["유통", "백화점", "마트", "편의점", "커머스", "쇼핑"]):
        return "유통·커머스 소비 플랫폼 기업"

    if contains_any(joined, ["렌탈", "생활가전", "정수기", "가전"]):
        return "생활가전·렌탈 기반 소비재 기업"

    if contains_any(joined, ["종이", "포장", "골판지", "제지"]):
        return "제지·포장재 제조 기업"

    if contains_any(joined, ["기계", "설비", "장비", "자동화", "로봇"]):
        return "산업기계·자동화 장비 기업"

    if contains_any(joined, ["폐기물", "환경", "수처리", "대기", "재활용", "탈질"]):
        return "환경·재활용·수처리 인프라 기업"

    if contains_any(joined, ["지주", "홀딩스"]):
        return "자회사 포트폴리오를 보유한 지주회사"

    industry = get_first_text(item, ["industry", "sector"])
    if industry:
        return f"{industry} 기반 사업 기업"

    market = get_first_text(item, ["market"])
    if market:
        return f"{market} 상장 기업"

    return "재무 기준을 통과한 국내 상장 기업"


def build_profiles(items, existing):
    profiles = {}

    if isinstance(existing, dict):
        # 기존 수동 수정값은 우선 보존
        for code, value in existing.items():
            if isinstance(value, str) and value.strip():
                profiles[code] = clean_text(value)

    updated_count = 0

    for item in items:
        if not isinstance(item, dict):
            continue

        code = get_code(item)
        if not code:
            continue

        if code in MANUAL_PROFILES:
            new_profile = MANUAL_PROFILES[code]
        elif code in profiles and profiles[code]:
            continue
        else:
            new_profile = infer_profile_by_keywords(item)

        if profiles.get(code) != new_profile:
            profiles[code] = new_profile
            updated_count += 1

    return profiles, updated_count


def main():
    print("회사 한줄 소개 생성 시작")
    print(f"시작 시간: {now_text()}")

    market_snapshot = read_json(MARKET_SNAPSHOT_PATH)

    if market_snapshot is None:
        raise FileNotFoundError(f"시장 스냅샷 파일을 찾을 수 없습니다: {MARKET_SNAPSHOT_PATH}")

    existing = read_json(COMPANY_PROFILES_PATH) or {}
    raw_items = get_items(market_snapshot)

    profiles, updated_count = build_profiles(raw_items, existing)

    write_json(COMPANY_PROFILES_PATH, profiles)

    print(f"written: {COMPANY_PROFILES_PATH}")
    print(f"items: {len(raw_items)}")
    print(f"profiles: {len(profiles)}")
    print(f"updated: {updated_count}")


if __name__ == "__main__":
    main()
