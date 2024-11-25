import json
from flask import Blueprint, request, jsonify, current_app
from modules.db.game_db import GameDBConnector

game_bp = Blueprint("game", __name__)

@game_bp.route('/selectstocks', methods=['POST'])
def selectstocks():
    db_connector = GameDBConnector()

    # App으로부터 sector 텍스트 받아오기
    sector = request.json.get("sector")

    # sector 텍스트가 없는 경우
    if not sector:
        return jsonify({"error": "No sector text provided"})

    try:
        # DB에서 sector에 따른 회사 가명(alias) 조회
        aliases = db_connector.select_sector_aliases(sector)

        # 가명 값이 없는 경우
        if not aliases:
            return jsonify({"success": False, "error": "Company alias not found"})
        else:
            # 가명 값이 있는 경우
            return jsonify({"success": True, "alias": aliases})

    except Exception as error:
        print(f"Error: {error}")
        return jsonify({'success': False, 'message': str(error)})

    finally:
        db_connector.close()

@game_bp.route("/save_check", methods=['POST'])
def save_check():
    db_connector = GameDBConnector()

    # App으로부터 user_id 받아오기
    idToken = request.json.get("token")

    # DB로부터 GAME_IDX 값 조회
    try:
        # DB로부터 받은 IDX 값에서 추출
        idx = db_connector.select_user_game_idx(idToken)[0]['GAME_IDX']
        return jsonify({'success': True, 'idx':idx})

    except Exception as error :
        # 예외가 발생한 경우 에러 메시지 반환
        print(f"Error:: {error}")
        return jsonify({'success': False, 'message':str(error)})

@game_bp.route("/start", methods=['POST'])
def start():
    db_connector = GameDBConnector()

    # App으로 부터 companyName 텍스트 가져오기
    alias = request.json.get("companyName")

    # 사용자 아이디 설정
    user_id = request.json.get("token")

    try:
        # DB에서 게임을 조회하기 위한 회사를 조회
        company_code = db_connector.select_game_company(alias)

        # DB에서 게임을 실행하기 위해 남아있는 게임을 조회
        game_idx = db_connector.select_game_generate(company_code)
        print("남아있는 게임 조회",game_idx)

        # 사용자에게 게임 저장
        db_connector.update_game_idx_user(user_id, game_idx)

        # 게임에 사용자 저장
        db_connector.update_game_user(user_id, game_idx)

        # 성공적으로 저장된 경우
        return jsonify({"message": "Game successfully started", "game_idx": game_idx}), 200

    except Exception as e:
        # 예외가 발생한 경우 에러 메시지 반환
        print("Error:", e)
        return jsonify({"error": "Failed to start the game", "details": str(e)}), 500
    
@game_bp.route("/app_bar", methods=['POST'])
def app_bar():
    db_connector = GameDBConnector()
    user_id = request.json.get('token')

    try:
        game_idx = db_connector.select_game_idx(user_id)
        if game_idx is None:
            return jsonify({"success": False, "error": "No game found for the specified user"}), 400

        game_info_all = db_connector.select_game_info_all(game_idx)
        company_alias = db_connector.select_stock_list_all(game_info_all[0]["COMPANY_CODE"])
        current_turn = game_info_all[0]["CURRENT_TURN"]-1

        game_detail_all = db_connector.select_game_detail_all(game_idx)
        game_detail_turn = game_detail_all[current_turn]["CURRENT_DETAIL_TURN"] -1

        chart_detail_all = db_connector.select_chart_detail_all(game_detail_all[game_detail_turn]["GAME_DETAIL_IDX"])
        chart_time = chart_detail_all[game_detail_turn]["CHART_TIME"]

        print("company_alias", company_alias)

        return jsonify({"success": True,"company_alias": company_alias, "current_turn": current_turn, "chart_time":chart_time})

    except Exception as error:
        print(f"Error: {error}")
        return jsonify({'success': False, 'message': str(error)})
    
    finally:
        db_connector.close()

@game_bp.route("bottom_bar", methods=['POST'])
def bottom_bar():
    db_connector = GameDBConnector()

    user_id = request.json.get('token')

    try:
        game_idx = db_connector.select_game_idx(user_id)
        if game_idx is None:
            return jsonify({"success": False, "error": "No game found for the specified user"}), 400

        game_info_all = db_connector.select_game_info_all(game_idx)
        current_turn = game_info_all[0]["CURRENT_TURN"]-1

        game_detail_all = db_connector.select_game_detail_all(game_idx)
        # 현재 턴의 CURRENT_DETAIL_TURN 가져오기
        current_detail_turn = game_detail_all[current_turn]["CURRENT_DETAIL_TURN"]

        game_detail_turn = game_detail_all[current_turn]["CURRENT_DETAIL_TURN"]
        current_money = game_detail_all[current_turn]["CURRENT_MONEY"]
        position_money = game_detail_all[current_turn]["POSITION_MONEY"]

        # 현재 차트 데이터 가져오기 (증가된 CURRENT_DETAIL_TURN에 따라 인덱스를 조정)
        current_game_detail_idx = game_detail_all[current_turn]["GAME_DETAIL_IDX"]
        chart_detail_all = db_connector.select_chart_detail_all(current_game_detail_idx)

        # 현재의 CURRENT_DETAIL_TURN에 해당하는 차트 데이터 가져오기
        if current_detail_turn > len(chart_detail_all):
            current_detail_turn = len(chart_detail_all)

        chart_current = chart_detail_all[current_detail_turn - 1]["CHART_CURRENT"]

        return jsonify({"success": True, "current_money":current_money, "position_money":position_money, "chart_current":chart_current, "game_detail_turn":game_detail_turn})

    except Exception as error:
        print(f"Error: {error}")
        return jsonify({'success': False, 'message': str(error)})
    
    finally:
        db_connector.close()


@game_bp.route("/chart", methods=['POST'])
def chart():
    db_connector = GameDBConnector()

    user_id = request.json.get("token")

    try:
        # 게임 인덱스 가져오기
        game_idx = db_connector.select_game_idx(user_id)

        # 게임 정보 가져오기
        game_info_all = db_connector.select_game_info_all(game_idx)
        current_turn = game_info_all[0]["CURRENT_TURN"] - 1
        print("chart 현재 턴:", current_turn)

        # 게임 상세 정보 가져오기
        game_detail_all = db_connector.select_game_detail_all(game_idx)
        print("game_detail_all:", game_detail_all)

        # 현재 턴의 CURRENT_DETAIL_TURN 가져오기
        current_detail_turn = game_detail_all[current_turn]["CURRENT_DETAIL_TURN"]

        # 현재 차트 데이터 가져오기 (증가된 CURRENT_DETAIL_TURN에 따라 인덱스를 조정)
        current_game_detail_idx = game_detail_all[current_turn]["GAME_DETAIL_IDX"]
        chart_detail_all = db_connector.select_chart_detail_all(current_game_detail_idx)

        # 현재의 CURRENT_DETAIL_TURN에 해당하는 차트 데이터 가져오기
        if current_detail_turn > len(chart_detail_all):
            current_detail_turn = len(chart_detail_all)

        chart_current = chart_detail_all[current_detail_turn - 1]["CHART_CURRENT"]
        print("현재 차트 가격:", chart_current)

        # 1턴부터 현재 턴까지의 모든 차트 데이터 가져오기
        all_chart_data = db_connector.select_chart_detail_range(game_idx, current_turn+1)
        print("all data", all_chart_data)

        # 일별 데이터를 누적하여 시가(Open), 종가(Close), 고가(High), 저가(Low) 계산
        daily_chart_data = []
        current_day_data = []

        for chart in all_chart_data:
            # 새로운 턴이 시작되면 하루를 마감하고 새로운 데이터를 추가
            if chart['GAME_DETAIL_IDX'] != current_game_detail_idx:
                if current_day_data:
                    open_price = current_day_data[0]["CHART_OPEN"]
                    close_price = current_day_data[-1]["CHART_CURRENT"]
                    high_price = max(item["CHART_HIGH"] for item in current_day_data)
                    low_price = min(item["CHART_LOW"] for item in current_day_data)

                    daily_chart_data.append({
                        "open": open_price,
                        "close": close_price,
                        "high": high_price,
                        "low": low_price
                    })
                current_day_data = []
                current_game_detail_idx = chart['GAME_DETAIL_IDX']

            # 현재 턴의 데이터를 추가
            current_day_data.append(chart)

        print("일별 데이터 :", current_day_data)

        # 마지막 남은 데이터를 추가
        if current_day_data:
            open_price = current_day_data[0]["CHART_OPEN"]
            close_price = current_day_data[-1]["CHART_CURRENT"]
            high_price = max(item["CHART_HIGH"] for item in current_day_data)
            low_price = min(item["CHART_LOW"] for item in current_day_data)

            daily_chart_data.append({
                "open": open_price,
                "close": close_price,
                "high": high_price,
                "low": low_price
            })

        print("해당 턴의 종가 : ",close_price)
        daily_money_data = []

        for turn_idx, daily_data in enumerate(daily_chart_data, start=1):
            turn_close_price = daily_data["close"]  # 각 턴의 종가 가져오기
            money_data = db_connector.select_money_turn(game_idx, turn_idx, turn_close_price)
            daily_money_data.extend(money_data)
            
            print("일별 차트 데이터", daily_chart_data)
            print("현재 데이터", current_day_data)
            print("일별 내 자산 데이터", daily_money_data)

        return jsonify({
            "success": True,
            "current_day_data": current_day_data,
            "chart_current": chart_current,
            "daily_chart_data": daily_chart_data,
            "daily_money" : daily_money_data
        })

    except Exception as error:
        print(f"Error: {error}")
        return jsonify({'success': False, 'message': str(error)}), 500

    finally:
        db_connector.close()


@game_bp.route("/next_detail_turn", methods=['POST'])
def next_detail_turn():

    db_connector = GameDBConnector()

    # App으로 부터 사용자 아이디 가져오기
    user_id = request.json.get("token")

    try:
        game_idx = db_connector.select_game_idx(user_id)

        game_info_all = db_connector.select_game_info_all(game_idx)
        current_turn = game_info_all[0]["CURRENT_TURN"]-1

        game_detail_all = db_connector.select_game_detail_all(game_idx)
        game_detail_idx = game_detail_all[current_turn]["GAME_DETAIL_IDX"]

        db_connector.update_next_detail_turn(game_detail_idx)
        return jsonify({'success': True, 'message': 'Detail Turn updated successfully'})

    except Exception as error:
        print(f"Error: {error}")
        return jsonify({'success': False, 'message': str(error)})
    
    finally:
        db_connector.close()

@game_bp.route("/next_turn", methods=['POST'])
def next_turn():
    db_connector = GameDBConnector()

    # App으로 부터 사용자 아이디 가져오기
    user_id = request.json.get("token")
    current_money = request.json.get("currentMoney")
    position_money = request.json.get("positionMoney")

    try:

        game_idx = db_connector.select_game_idx(user_id)

        db_connector.update_next_turn(game_idx)

        # 전체 게임 정보 가져오기
        game_info_all = db_connector.select_game_info_all(game_idx)

        # 현재 턴 정보 확인
        current_turn = game_info_all[0]["CURRENT_TURN"] - 1

        # 실제 턴
        real_turn = current_turn + 1

        # 게임 세부 정보 가져오기
        game_detail_all = db_connector.select_game_detail_all(game_idx)


        game_detail_idx = game_detail_all[current_turn]["GAME_DETAIL_IDX"]

        # 뉴스 리스트 가져오기
        news_list = db_connector.select_news_list(game_idx, current_turn)
        print(f"news_list: {news_list}, length: {len(news_list)}")

        if current_turn != 0:
            current_turn -= 1

        warning_news = news_list[current_turn]["OUTNEWS_TITLE"]

        db_connector.update_money(game_detail_idx, current_money, position_money)

        print("리얼 턴 :", real_turn)

        return jsonify({'success': True, 'warning_news': warning_news, "current_turn" : real_turn})

    except Exception as error:
        print(f"Error: {error}")
        return jsonify({'success': False, 'message': str(error)})
    
    finally:
        db_connector.close()

@game_bp.route("/news", methods=['POST'])
def news():

    db_connector = GameDBConnector()

    # App으로 부터 사용자 아이디 가져오기
    user_id = request.json.get("token")

    try:
        game_idx = db_connector.select_game_idx(user_id)

        game_info_all = db_connector.select_game_info_all(game_idx)
        current_turn = game_info_all[0]["CURRENT_TURN"]

        news_list = db_connector.select_news_list(game_idx, current_turn)
        return jsonify({'success': True, "news_list":news_list})

    except Exception as error:
        print(f"Error: {error}")
        return jsonify({'success': False, 'message': str(error)})
    
    finally:
        db_connector.close()

@game_bp.route("/report", methods=['POST'])
def report():
    db_connector = GameDBConnector()

    # App으로 부터 사용자 아이디 가져오기
    user_id = request.json.get("token")

    try:
        game_idx = db_connector.select_game_idx(user_id)

        game_info_all = db_connector.select_game_info_all(game_idx)
        current_turn = game_info_all[0]["CURRENT_TURN"]

        report_list = db_connector.select_report_list(game_idx, current_turn)
        return jsonify({'success': True, "report_list":report_list})

    except Exception as error:
        print(f"Error: {error}")
        return jsonify({'success': False, 'message': str(error)})
    
    finally:
        db_connector.close()

@game_bp.route("/financial", methods=['POST'])
def financial():
    db_connector = GameDBConnector()

    # App으로 부터 사용자 아이디 가져오기
    user_id = request.json.get("token")

    try:
        game_idx = db_connector.select_game_idx(user_id)

        game_info_all = db_connector.select_game_info_all(game_idx)
        current_turn = game_info_all[0]["CURRENT_TURN"]

        financial = db_connector.select_financial_list(game_idx, current_turn)
        return jsonify({'success': True, "financial":financial})

    except Exception as error:
        print(f"Error: {error}")
        return jsonify({'success': False, 'message': str(error)})
    
    finally:
        db_connector.close()

@game_bp.route("/buy", methods=['POST'])
def buy():

    db_connector = GameDBConnector()

    # App으로 부터 사용자 아이디 가져오기
    user_id = request.json.get("token")
    quantity = request.json.get("quantity")
    total = request.json.get("total")

    try:
        game_idx = db_connector.select_game_idx(user_id)
        game_info_all = db_connector.select_game_info_all(game_idx)
        current_turn = game_info_all[0]["CURRENT_TURN"]-1

        game_detail_all = db_connector.select_game_detail_all(game_idx)
        game_detail_idx = game_detail_all[current_turn]["GAME_DETAIL_IDX"]

        db_connector.update_buy(game_detail_idx, current_money=total, position_money=quantity)

        db_connector.update_next_detail_turn(game_detail_idx)

        return jsonify({'success': True, 'message': 'Turn updated successfully'})

    except Exception as error:

        print(f"Error: {error}")
        return jsonify({'success': False, 'message': str(error)})
    
    finally:
        db_connector.close()

@game_bp.route("/sell", methods=['POST'])
def sell():

    db_connector = GameDBConnector()

    # App으로 부터 사용자 아이디 가져오기
    user_id = request.json.get("token")
    quantity = request.json.get("quantity")
    total = request.json.get("total")

    try:
        game_idx = db_connector.select_game_idx(user_id)
        game_info_all = db_connector.select_game_info_all(game_idx)
        current_turn = game_info_all[0]["CURRENT_TURN"]-1

        game_detail_all = db_connector.select_game_detail_all(game_idx)
        game_detail_idx = game_detail_all[current_turn]["GAME_DETAIL_IDX"]

        db_connector.update_sell(game_detail_idx, current_money=total, position_money=quantity)

        db_connector.update_next_detail_turn(game_detail_idx)

        return jsonify({'success': True, 'message': 'Turn updated successfully'})

    except Exception as error:

        print(f"Error: {error}")
        return jsonify({'success': False, 'message': str(error)})
    
    finally:
        db_connector.close()

@game_bp.route("/game_end", methods=['POST'])
def game_end():

    db_connector = GameDBConnector()

    # App으로 부터 사용자 아이디 가져오기
    user_id = request.json.get("token")
    profitRate = request.json.get("profitRate")

    print("profitRate", profitRate)

    score = int(profitRate * 100)
    print("score", score)

    try:
        if score > 0 :
            db_connector.update_game_success(user_id, score)
        else :
            db_connector.update_game_fail(user_id, score)

        db_connector.update_game_reset(user_id)

        return jsonify({'success': True, 'message': 'End updated successfully'})

    except Exception as error:

        print(f"Error: {error}")
        return jsonify({'success': False, 'message': str(error)})
    
    finally:
        db_connector.close()