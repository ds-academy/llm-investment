import os
import pymysql
import json
import random
from modules.db.game_db import GameDBConnector

def get_company_code(company_name: str) -> str:
    """
    주어진 회사 이름으로 COMPANY_CODE를 조회합니다.
    :param company_name: 회사 이름
    :return: COMPANY_CODE 또는 None
    """
    db_connector = GameDBConnector()
    query = "SELECT STOCK_CODE FROM STOCK_LIST_TB WHERE COMPANY_NAME=%s"
    
    try:
        result = db_connector.select(query, (company_name,))
        if result:
            return result[0]["STOCK_CODE"]
        else:
            raise ValueError(f"'{company_name}'에 해당하는 회사 코드를 찾을 수 없습니다.")
    except Exception as e:
        print(f"회사 코드를 가져오는 중 오류 발생: {e}")
        return None
    finally:
        db_connector.close()

def insert_chart_detail(db_connector, chart_data, chart_time):
    query = """
        INSERT INTO CHART_DETAIL_TB (CHART_OPEN, CHART_HIGH, CHART_LOW, CHART_CLOSE, CHART_TIME)
        VALUES (%s, %s, %s, %s, %s)
    """
    params = (
        chart_data["start"],
        chart_data["high"],
        chart_data["low"],
        chart_data["current"],
        chart_time
    )
    
    try:
        db_connector.execute_query(query, params)
        db_connector.connection.commit()
        
        last_insert_id = db_connector.get_last_insert_id()
        return last_insert_id
        
    except Exception as e:
        print(f"Error inserting data: {e}")
        db_connector.connection.rollback()
        return None

def insert_game_detail(db_connector, chart_detail_idx, game_turn, game_data):
    query = """
        INSERT INTO GAME_DETAIL_TB (CHART_DETAIL_IDX, INNEWS_TITLE, INNEWS_INFO, OUTNEWS_TITLE, OUTNEWS_INFO, REPORT_TITLE, REPORT_INFO, FINANCIAL_STATEMENTS_TITLE, FINANCIAL_STATEMENTS_INFO, CURRENT_MONEY, POSITION_MONEY, GAME_TURN)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    innews_title = game_data["category"]["innews"]["title"]
    innews_info = game_data["category"]["innews"]["info"]
    outnews_title = game_data["category"]["outnews"]["title"]
    outnews_info = game_data["category"]["outnews"]["info"]
    report_title = game_data["category"]["report"]["title"]
    report_info = game_data["category"]["report"]["info"]
    financial_statements_title = game_data["category"]["financial_statements"]["title"]
    financial_statements_info = game_data["category"]["financial_statements"]["info"]
    current_money = game_data.get("current_money", 100000000)
    position_money = game_data.get("position_money", 0)
    params = (chart_detail_idx, innews_title, innews_info, outnews_title, outnews_info, report_title, report_info, financial_statements_title, financial_statements_info, current_money, position_money, game_turn)
    db_connector.execute_query(query, params)
    db_connector.connection.commit()

    return db_connector.get_last_insert_id()

def insert_game_info(db_connector, user_id, sector, company_code, game_detail_idx, game_date):
    query = """
        INSERT INTO GAME_INFO_TB (USER_ID, SECTOR, COMPANY_CODE, GAME_DETAIL_IDX, GAME_DATE)
        VALUES (%s, %s, %s, %s, %s)
    """
    params = (user_id, sector, company_code, game_detail_idx, game_date)
    db_connector.execute_query(query, params)
    return db_connector.connection.insert_id()

def save_game_to_db(game_data, user_id, sector, company_code, game_date):
    db_connector = GameDBConnector()

    try:
        for turn_data in game_data:
            turn = turn_data["turn"]

            chart_detail_ids = []
            for chart_time, chart in turn_data["chart"]["times"].items():
                chart_detail_idx = insert_chart_detail(db_connector, chart, chart_time)
                chart_detail_ids.append(chart_detail_idx)
                print(chart_detail_ids)

            game_detail_idx = insert_game_detail(db_connector, chart_detail_ids[0], turn, turn_data)
            print(game_detail_idx)

            if turn == 1:
                insert_game_info(
                    db_connector,
                    user_id=user_id,
                    sector=sector,
                    company_code=company_code,
                    game_detail_idx=game_detail_idx,
                    game_date=game_date
                )

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
    game_date = "2024-10-24"

    company_code = get_company_code(company_name)
    if not company_code:
        raise ValueError(f"'{company_name}'에 대한 COMPANY_CODE를 찾을 수 없습니다.")

    # game_data = generate_game_scenario(api_key, company_name)
    game_data = []

    print(game_data)
    save_game_to_db(game_data, user_id, sector, company_code, game_date)
