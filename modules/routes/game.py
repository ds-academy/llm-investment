import json
from flask import Blueprint, request, jsonify, current_app
from modules.db.game_db import GameDBConnector
from modules.llm.chat_gpt import GPTModel

game_bp = Blueprint("game", __name__)

def get_gpt_model():
    api_key = current_app.config["OPENAI_API_KEY"]
    model_id = current_app.config["GPT_MODEL_ID"]
    return GPTModel(api_key=api_key, model_id=model_id)


def parse_model_response(response):
    try:
        if isinstance(response, str):
            parsed_response = json.loads(response)
        elif isinstance(response, dict):
            parsed_response = response
        else:
            raise ValueError("Unexpected response type")

        # 필수 필드 확인
        required_fields = ['answer', 'user_invest_type', 'confidence']
        for field in required_fields:
            if field not in parsed_response:
                raise KeyError(f"Missing required field: {field}")

        return parsed_response, None

    except json.JSONDecodeError as e:
        error = {
            "error_type": "JSONDecodeError",
            "error_message": "응답을 JSON으로 파싱할 수 없습니다.",
            "original_response": response[:100] + "..." if len(response) > 100 else response
        }
        return None, error

    except (ValueError, KeyError) as e:
        error = {
            "error_type": type(e).__name__,
            "error_message": str(e),
            "original_response": response if isinstance(response, str) else str(response)
        }
        return None, error

@game_bp.route("/next-turn", methods=["POST"])
def next_turn():
    try:
        # 사용자로부터 받은 데이터
        data = request.get_json()
        game_detail_idx = data.get("game_detail_idx")
        if not game_detail_idx:
            return jsonify({"error": "game_detail_idx가 제공되지 않았습니다."}), 400

        # 현재 게임 세부 정보를 가져와 확인
        db_connector = GameDBConnector()
        game_detail_query = "SELECT GAME_TURN FROM GAME_DETAIL_TB WHERE GAME_DETAIL_IDX = %s"
        game_detail = db_connector.select(game_detail_query, (game_detail_idx,))
        
        if not game_detail:
            return jsonify({"error": "유효하지 않은 게임 세부 정보입니다."}), 400

        current_turn = game_detail[0]["GAME_TURN"]
        next_turn = current_turn + 1

        # ChatGPT 모델을 사용하여 다음 턴의 데이터 생성
        gpt_model = get_gpt_model()
        prompt = f"Create the next turn scenario for game_detail_idx: {game_detail_idx}, current_turn: {current_turn}."
        model_response = gpt_model.generate(prompt)

        # 응답 데이터 파싱
        parsed_response, error = parse_model_response(model_response)
        if error:
            return jsonify({"error": error["error_message"]}), 500

        game_data = parsed_response

        # 1. CHART_DETAIL_TB에 새로운 차트 데이터 저장
        chart_detail_ids = []
        for time, chart in game_data["chart"]["times"].items():
            chart_insert_query = """
                INSERT INTO CHART_DETAIL_TB (CHART_OPEN, CHART_HIGH, CHART_LOW, CHART_CLOSE, CHART_TIME)
                VALUES (%s, %s, %s, %s, %s)
            """
            chart_params = (
                chart["start"],
                chart["high"],
                chart["low"],
                chart["current"],
                time,
            )
            db_connector.insert(chart_insert_query, chart_params)
            chart_detail_ids.append(db_connector.get_last_insert_id())

        # 2. GAME_DETAIL_TB에 새로운 게임 세부 정보 저장
        for idx, chart_detail_id in enumerate(chart_detail_ids):
            game_detail_query = """
                INSERT INTO GAME_DETAIL_TB (CHART_DETAIL_IDX, NEWS, REPORT, FINANCIAL_STATEMENTS, CURRENT_MONEY, POSITION_MONEY, GAME_TURN)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            game_detail_params = (
                chart_detail_id,
                game_data["category"]["news"]["info"],
                game_data["category"]["report"]["info"],
                game_data["category"]["financial_statements"]["info"],
                game_data.get("current_money", 0),
                game_data.get("position_money", 0),
                next_turn,  # 다음 턴
            )
            db_connector.insert(game_detail_query, game_detail_params)

        response = {
            "message": "다음 턴 데이터 생성 및 저장 완료",
            "game_data": game_data,
            "next_turn": next_turn,
        }
        return jsonify(response)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@game_bp.route("/create-game", methods=["POST"])
def create_game():
    try:
        # 사용자로부터 받은 데이터
        data = request.get_json()
        user_id = data.get("user_id")
        company_code = data.get("company_code")
        sector = data.get("sector")
        game_date = data.get("game_date")

        if not user_id or not company_code or not sector or not game_date:
            return jsonify({"error": "필수 정보(user_id, company_code, sector, game_date)가 제공되지 않았습니다."}), 400

        # ChatGPT 모델을 사용하여 데이터 생성
        gpt_model = get_gpt_model()
        prompt = f"Create a game scenario for user_id: {user_id}, company_code: {company_code}."
        model_response = gpt_model.generate(prompt)

        # 응답 데이터 파싱
        parsed_response, error = parse_model_response(model_response)
        if error:
            return jsonify({"error": error["error_message"]}), 500

        game_data = parsed_response

        db_connector = GameDBConnector()

        # 1. CHART_DETAIL_TB에 데이터 저장
        chart_detail_ids = []
        for time, chart in game_data["chart"]["times"].items():
            chart_insert_query = """
                INSERT INTO CHART_DETAIL_TB (CHART_OPEN, CHART_HIGH, CHART_LOW, CHART_CLOSE, CHART_TIME)
                VALUES (%s, %s, %s, %s, %s)
            """
            chart_params = (
                chart["start"],
                chart["high"],
                chart["low"],
                chart["current"],
                time,
            )
            db_connector.insert(chart_insert_query, chart_params)
            chart_detail_ids.append(db_connector.get_last_insert_id())

        # 2. GAME_DETAIL_TB에 데이터 저장
        for idx, chart_detail_id in enumerate(chart_detail_ids):
            game_detail_query = """
                INSERT INTO GAME_DETAIL_TB (CHART_DETAIL_IDX, NEWS, REPORT, FINANCIAL_STATEMENTS, CURRENT_MONEY, POSITION_MONEY, GAME_TURN)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            game_detail_params = (
                chart_detail_id,
                game_data["category"]["news"]["info"],
                game_data["category"]["report"]["info"],
                game_data["category"]["financial_statements"]["info"],
                game_data.get("current_money", 0),
                game_data.get("position_money", 0),
                idx + 1,  # 각 차트에 해당하는 턴
            )
            db_connector.insert(game_detail_query, game_detail_params)
            game_detail_id = db_connector.get_last_insert_id()

        # 3. GAME_INFO_TB에 데이터 저장 (게임 정보에 대한 기본 데이터 저장)
        game_info_query = """
            INSERT INTO GAME_INFO_TB (USER_ID, SECTOR, COMPANY_CODE, GAME_DETAIL_IDX, GAME_DATE)
            VALUES (%s, %s, %s, %s, %s)
        """
        game_info_params = (
            user_id,
            sector,
            company_code,
            game_detail_id,
            game_date,
        )
        db_connector.insert(game_info_query, game_info_params)

        response = {
            "message": "게임 데이터 생성 및 저장 완료",
            "game_data": game_data
        }
        return jsonify(response)

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

    


def init_chat_module(app):
    app.register_blueprint(game_bp)
    if "OPENAI_API_KEY" not in app.config:
        raise ValueError("OPENAI_API_KEY 환경 변수가 설정되지 않았습니다.")
