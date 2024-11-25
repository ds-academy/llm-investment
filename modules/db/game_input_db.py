import os
import pymysql
import json
import random
from modules.db.game_db import GameDBConnector
import datetime

# 1. 회사 코드를 조회하는 함수
def get_company_code(company_name: str) -> str:
    db_connector = GameDBConnector()
    query = "SELECT COMPANY_CODE FROM STOCK_LIST_TB WHERE COMPANY_NAME=%s"
    try:
        result = db_connector.select(query, (company_name,))
        if result:
            return result[0]["COMPANY_CODE"]
        else:
            raise ValueError(f"'{company_name}'에 해당하는 회사 코드를 찾을 수 없습니다.")
    except Exception as e:
        print(f"회사 코드를 가져오는 중 오류 발생: {e}")
        return None
    finally:
        db_connector.close()


# 2. 차트 데이터를 저장하는 함수
def insert_chart_detail(db_connector, game_detail_idx, chart_data, chart_time):
    query = """
        INSERT INTO CHART_DETAIL_TB (GAME_DETAIL_IDX, CHART_OPEN, CHART_HIGH, CHART_LOW, CHART_CURRENT, CHART_TIME)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    params = (
        game_detail_idx,
        chart_data["start"],
        chart_data["high"],
        chart_data["low"],
        chart_data["current"],
        chart_time
    )
    
    try:
        db_connector.execute_query(query, params)
        db_connector.connection.commit()
        return db_connector.get_last_insert_id()
    except Exception as e:
        print(f"차트 데이터 삽입 중 오류 발생: {e}")
        db_connector.connection.rollback()
        return None


# 3. 게임 상세 데이터를 저장하는 함수
def insert_game_detail(db_connector, game_idx, game_data):
    query = """
        INSERT INTO GAME_DETAIL_TB (GAME_IDX, INNEWS_TITLE, INNEWS_INFO, OUTNEWS_TITLE, OUTNEWS_INFO,
                                    REPORT_TITLE, REPORT_INFO, FINANCIAL_STATEMENTS_TITLE, FINANCIAL_STATEMENTS_INFO,
                                    CURRENT_MONEY, POSITION_MONEY)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = (
        game_idx,
        game_data["category"]["innews"]["title"],
        game_data["category"]["innews"]["info"],
        game_data["category"]["outnews"]["title"],
        game_data["category"]["outnews"]["info"],
        game_data["category"]["report"]["title"],
        game_data["category"]["report"]["info"],
        game_data["category"]["financial_statements"]["title"],
        game_data["category"]["financial_statements"]["info"],
        game_data.get("current_money", 100000000),
        game_data.get("position_money", 0),
    )

    # 디버깅을 위해 params 출력
    print(f"쿼리 파라미터: {params}")
    
    db_connector.execute_query(query, params)
    db_connector.connection.commit()
    return db_connector.get_last_insert_id()


# 4. 게임 정보를 저장하는 함수
def insert_game_info(db_connector, user_id, company_code, game_date):
    query = """
        INSERT INTO GAME_INFO_TB (USER_ID, COMPANY_CODE, CURRENT_TURN, GAME_DATE)
        VALUES (%s, %s, %s, %s)
    """
    params = (user_id, company_code, 1, game_date)
    db_connector.execute_query(query, params)
    return db_connector.get_last_insert_id()


# 5. 전체 게임 데이터를 데이터베이스에 저장하는 함수
def save_game_to_db(game_data, company_name):
    db_connector = GameDBConnector()
    game_date = datetime.datetime.now().date()

    try:
        # 회사 코드를 가져옴
        company_code = get_company_code(company_name)

        # 1. GAME_INFO_TB에 게임 정보 삽입
        game_idx = insert_game_info(db_connector, user_id, company_code, game_date)
        print(f"게임 정보 저장 완료, GAME_IDX: {game_idx}")

        # 2. 각 턴에 대한 상세 데이터 삽입
        for turn_data in game_data:

            # 2-1. GAME_DETAIL_TB에 게임 상세 데이터 삽입
            game_detail_idx = insert_game_detail(db_connector, game_idx, turn_data)
            print(f"게임 상세 데이터 저장 완료, GAME_DETAIL_IDX: {game_detail_idx}")

            # 2-2. CHART_DETAIL_TB에 차트 데이터 삽입
            for chart_time, chart in turn_data["chart"]["times"].items():
                chart_detail_idx = insert_chart_detail(db_connector, game_detail_idx, chart, chart_time)
                print(f"차트 데이터 저장 완료, CHART_DETAIL_IDX: {chart_detail_idx}")

        db_connector.connection.commit()

    except Exception as e:
        print(f"게임 데이터를 저장하는 중 오류 발생: {e}")
        db_connector.connection.rollback()
    
    finally:
        db_connector.close()

if __name__ == "__main__":
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY 환경 변수가 설정되지 않았습니다.")

    user_id = "UNNAMED"
    sector = input("섹터를 입력하세요: ")
    company_name = input("회사명을 입력하세요: ")

    company_code = get_company_code(company_name)
    if not company_code:
        raise ValueError(f"'{company_name}'에 대한 COMPANY_CODE를 찾을 수 없습니다.")

    # game_data = generate_game_scenario(api_key, company_name)
    game_data = [
  {
    "turn": 1,
    "description": "펩트론의 신약 개발 성공 소식으로 주가가 상승하고 있습니다.",
    "company": "펩트론",
    "chart": {
      "times": {
        "09:00": { "start": 15000, "high": 15200, "low": 14800, "current": 15100 },
        "11:00": { "start": 15050, "high": 15250, "low": 14850, "current": 14900 },
        "13:00": { "start": 14950, "high": 15150, "low": 14750, "current": 15000 },
        "15:00": { "start": 14950, "high": 15150, "low": 14750, "current": 15100 }
      }
    },
    "category": {
      "innews": {
        "title": "신약 개발 성공",
        "info": "펩트론이 새로운 신약 개발에 성공하여 시장의 기대를 모으고 있습니다."
      },
      "outnews": { "title": "", "info": "" },
      "financial_statements": {
        "title": "펩트론 1턴 재무제표",
        "info": "매출액: 500억원, 순이익: 50억원, 부채비율: 30%. 신약 출시로 매출 증가 예상."
      },
      "report": {
        "title": "신약 개발 성공의 영향",
        "info": "신약 출시로 인한 매출 증가가 예상됩니다."
      }
    },
    "confidence": 0.90
  },
  {
    "turn": 2,
    "description": "펩트론이 글로벌 제약사와 파트너십을 체결하여 주가가 상승하고 있습니다.",
    "company": "펩트론",
    "chart": {
      "times": {
        "09:00": { "start": 15200, "high": 15400, "low": 15000, "current": 15300 },
        "11:00": { "start": 15250, "high": 15450, "low": 15050, "current": 15100 },
        "13:00": { "start": 15150, "high": 15350, "low": 14950, "current": 15200 },
        "15:00": { "start": 15150, "high": 15350, "low": 14950, "current": 15000 }
      }
    },
    "category": {
      "innews": {
        "title": "글로벌 파트너십 체결",
        "info": "펩트론이 글로벌 제약사와 전략적 파트너십을 맺었습니다."
      },
      "outnews": { "title": "", "info": "" },
      "financial_statements": {
        "title": "펩트론 2턴 재무제표",
        "info": "매출액: 520억원, 순이익: 60억원, 부채비율: 29%. 글로벌 네트워크 확대로 시장 진출 기대."
      },
      "report": {
        "title": "파트너십의 영향",
        "info": "글로벌 네트워크 확대로 시장 진출이 용이해질 것으로 예상됩니다."
      }
    },
    "confidence": 0.92
  },
  {
    "turn": 3,
    "description": "정부의 바이오 산업 지원 발표로 기대감이 높아지고 있습니다.",
    "company": "펩트론",
    "chart": {
      "times": {
        "09:00": { "start": 14900, "high": 15100, "low": 14700, "current": 15000 },
        "11:00": { "start": 14950, "high": 15150, "low": 14750, "current": 14800 },
        "13:00": { "start": 14850, "high": 15050, "low": 14650, "current": 14950 },
        "15:00": { "start": 14900, "high": 15100, "low": 14700, "current": 14750 }
      }
    },
    "category": {
      "innews": {
        "title": "바이오 산업 지원 발표",
        "info": "정부가 바이오 산업에 대한 대규모 지원 정책을 발표하였습니다."
      },
      "outnews": {
        "title": "정부의 바이오 산업 투자 확대",
        "info": "정부가 바이오 기업에 대한 투자와 지원을 대폭 확대하기로 결정하였습니다."
      },
      "financial_statements": {
        "title": "펩트론 3턴 재무제표",
        "info": "매출액: 510억원, 순이익: 55억원, 부채비율: 31%. 정책 지원으로 자금 조달 개선."
      },
      "report": {
        "title": "정책 지원의 영향",
        "info": "자금 지원으로 연구 개발이 촉진될 것으로 예상됩니다."
      }
    },
    "confidence": 0.88
  },
  {
    "turn": 4,
    "description": "정부 지원 정책의 영향으로 주가가 급등하고 있습니다.",
    "company": "펩트론",
    "chart": {
      "times": {
        "09:00": { "start": 15000, "high": 19500, "low": 15000, "current": 19000 },
        "11:00": { "start": 19050, "high": 24000, "low": 19050, "current": 23500 },
        "13:00": { "start": 23550, "high": 29000, "low": 23550, "current": 28500 },
        "15:00": { "start": 28550, "high": 34000, "low": 28550, "current": 33500 }
      }
    },
    "category": {
      "innews": {
        "title": "정부 지원 효과 발현",
        "info": "정부의 지원 정책으로 펩트론의 성장 가능성이 부각되고 있습니다."
      },
      "outnews": { "title": "", "info": "" },
      "financial_statements": {
        "title": "펩트론 4턴 재무제표",
        "info": "매출액: 550억원, 순이익: 70억원, 부채비율: 29%. 성장 가능성 확대."
      },
      "report": {
        "title": "주가 급등의 원인",
        "info": "투자 심리 개선으로 인한 매수세가 증가하였습니다."
      }
    },
    "confidence": 0.95
  },
  {
    "turn": 5,
    "description": "글로벌 경제 불안으로 주가가 하락하고 있습니다.",
    "company": "펩트론",
    "chart": {
      "times": {
        "09:00": { "start": 33500, "high": 34000, "low": 33000, "current": 33250 },
        "11:00": { "start": 33300, "high": 33800, "low": 32800, "current": 33100 },
        "13:00": { "start": 33150, "high": 33650, "low": 32650, "current": 32900 },
        "15:00": { "start": 32950, "high": 33450, "low": 32450, "current": 32700 }
      }
    },
    "category": {
      "innews": {
        "title": "글로벌 경제 불안",
        "info": "세계 경제의 불확실성이 커지며 투자 심리가 위축되고 있습니다."
      },
      "outnews": { "title": "", "info": "" },
      "financial_statements": {
        "title": "펩트론 5턴 재무제표",
        "info": "매출액: 540억원, 순이익: 65억원, 부채비율: 30%. 글로벌 경기 둔화로 매출 감소."
      },
      "report": {
        "title": "경제 불안의 영향",
        "info": "전반적인 시장 하락세에 동조하고 있습니다."
      }
    },
    "confidence": 0.80
  },
  {
    "turn": 6,
    "description": "환율 변동으로 주가가 하락하고 있습니다.",
    "company": "펩트론",
    "chart": {
      "times": {
        "09:00": { "start": 32750, "high": 33000, "low": 32500, "current": 32600 },
        "11:00": { "start": 32650, "high": 32900, "low": 32400, "current": 32750 },
        "13:00": { "start": 32700, "high": 32950, "low": 32450, "current": 32500 },
        "15:00": { "start": 32550, "high": 32800, "low": 32300, "current": 32400 }
      }
    },
    "category": {
      "innews": {
        "title": "환율 변동 영향",
        "info": "환율 변동으로 수출 기업들의 수익성이 악화되고 있습니다."
      },
      "outnews": { "title": "", "info": "" },
      "financial_statements": {
        "title": "펩트론 6턴 재무제표",
        "info": "매출액: 530억원, 순이익: 60억원, 부채비율: 31%. 환율 변동으로 수익성 악화."
      },
      "report": {
        "title": "환율 변동의 영향",
        "info": "해외 매출에 부정적 영향이 예상됩니다."
      }
    },
    "confidence": 0.78
  },
  {
    "turn": 7,
    "description": "펩트론의 신제품 출시 소식으로 주가가 상승하고 있습니다.",
    "company": "펩트론",
    "chart": {
      "times": {
        "09:00": { "start": 32500, "high": 33000, "low": 32500, "current": 32750 },
        "11:00": { "start": 32700, "high": 33200, "low": 32700, "current": 32550 },
        "13:00": { "start": 32600, "high": 33100, "low": 32600, "current": 32800 },
        "15:00": { "start": 32750, "high": 33250, "low": 32750, "current": 33050 }
      }
    },
    "category": {
      "innews": {
        "title": "신제품 출시",
        "info": "펩트론이 새로운 제품을 출시하여 시장의 호응을 얻고 있습니다."
      },
      "outnews": { "title": "", "info": "" },
      "financial_statements": {
        "title": "펩트론 7턴 재무제표",
        "info": "매출액: 540억원, 순이익: 65억원, 부채비율: 31%. 신제품 출시로 매출 확대."
      },
      "report": {
        "title": "신제품 출시의 영향",
        "info": "매출 증대와 브랜드 이미지 향상이 기대됩니다."
      }
    },
    "confidence": 0.85
  },
  {
    "turn": 8,
    "description": "회사 내부 분쟁 소식으로 주가가 하락하고 있습니다.",
    "company": "펩트론",
    "chart": {
      "times": {
        "09:00": { "start": 33100, "high": 33300, "low": 32900, "current": 33000 },
        "11:00": { "start": 33050, "high": 33250, "low": 32850, "current": 32900 },
        "13:00": { "start": 32950, "high": 33150, "low": 32750, "current": 32800 },
        "15:00": { "start": 32850, "high": 33050, "low": 32650, "current": 32700 }
      }
    },
    "category": {
      "innews": {
        "title": "내부 분쟁 발생",
        "info": "경영진 간의 갈등으로 회사 내부 분쟁이 발생하였습니다."
      },
      "outnews": { "title": "", "info": "" },
      "financial_statements": {
        "title": "펩트론 8턴 재무제표",
        "info": "매출액: 530억원, 순이익: 60억원, 부채비율: 32%. 내부 갈등으로 경영 안정성 악화."
      },
      "report": {
        "title": "내부 분쟁의 영향",
        "info": "경영 안정성에 대한 우려가 커지고 있습니다."
      }
    },
    "confidence": 0.75
  },
  {
    "turn": 9,
    "description": "법적 분쟁 발생으로 주가가 하락하고 있습니다.",
    "company": "펩트론",
    "chart": {
      "times": {
        "09:00": { "start": 32750, "high": 32950, "low": 32550, "current": 32650 },
        "11:00": { "start": 32700, "high": 32900, "low": 32500, "current": 32500 },
        "13:00": { "start": 32550, "high": 32750, "low": 32350, "current": 32400 },
        "15:00": { "start": 32450, "high": 32650, "low": 32250, "current": 32300 }
      }
    },
    "category": {
      "innews": {
        "title": "법적 분쟁 발생",
        "info": "특허 침해 소송으로 법적 분쟁이 발생하였습니다."
      },
      "outnews": { "title": "", "info": "" },
      "financial_statements": {
        "title": "펩트론 9턴 재무제표",
        "info": "매출액: 520억원, 순이익: 55억원, 부채비율: 32%. 법적 비용 증가로 수익성 악화."
      },
      "report": {
        "title": "법적 분쟁의 영향",
        "info": "소송 비용 증가와 이미지 손상이 우려됩니다."
      }
    },
    "confidence": 0.73
  },
  {
    "turn": 10,
    "description": "펩트론의 혁신적인 연구 결과 발표로 주가가 상승하고 있습니다.",
    "company": "펩트론",
    "chart": {
      "times": {
        "09:00": { "start": 32400, "high": 32900, "low": 32400, "current": 32650 },
        "11:00": { "start": 32600, "high": 33100, "low": 32600, "current": 32450 },
        "13:00": { "start": 32500, "high": 33000, "low": 32500, "current": 32700 },
        "15:00": { "start": 32650, "high": 33150, "low": 32650, "current": 32950 }
      }
    },
    "category": {
      "innews": {
        "title": "혁신 연구 결과 발표",
        "info": "펩트론이 새로운 치료 분야에서 혁신적인 연구 결과를 발표하였습니다."
      },
      "outnews": { "title": "", "info": "" },
      "financial_statements": {
        "title": "펩트론 10턴 재무제표",
        "info": "매출액: 540억원, 순이익: 65억원, 부채비율: 31%. 혁신 기술로 성장 가능성 강화."
      },
      "report": {
        "title": "연구 성과의 영향",
        "info": "미래 성장 가능성이 높아져 투자자들의 관심이 증가하고 있습니다."
      }
    },
    "confidence": 0.88
  }
]



    print(game_data)
    save_game_to_db(game_data, company_name)