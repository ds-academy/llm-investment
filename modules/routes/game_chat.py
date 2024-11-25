import json
from flask import Blueprint, request, jsonify, current_app
from modules.db.game_chat_db import GameChatDBConnector
from modules.llm.chat_gpt import GPTModel

game_chat_bp = Blueprint("game_chat", __name__)

def get_gpt_model():
    api_key = current_app.config["OPENAI_API_KEY"]
    print('api_key = ', api_key)
    model_id = current_app.config["GPT_MODEL_ID"]
    print('model_id = ', model_id)
    return GPTModel(api_key=api_key, model_id=model_id)

@game_chat_bp.route('/send', methods=['POST'])
def send_message():
    db_connector = GameChatDBConnector()
    try:
        user_id = request.json.get("user_id")
        message = request.json.get("message")
        sender = request.json.get("sender", "user")

        # 게임 ID 조회
        game_idx = db_connector.select_game_idx(user_id)
        db_connector.save_conversation(game_idx, sender, message)

        # 메시지 저장 (사용자 메시지)
        if message.startswith("Tip:"):
            tip_name = message.split("Tip:")[1].split("Detail:")[0].strip()
            if "Detail:" in message:  # 세부 항목 클릭 시
                detail_name = message.split("Detail:")[1].strip()
                response = db_connector.get_answer(tip_name, detail_name)
                print("응답 형태", response)
                db_connector.save_conversation(game_idx, 'bot', response)
            else:  # TIP 클릭 시
                response = db_connector.get_details(tip_name)
                print("응답 형태", response)
                db_connector.save_conversation(game_idx, 'bot', response)
            return jsonify({"success": True, "response": response})
        else:
            # GPT 모델 연동
            gpt_model = get_gpt_model()
            response = gpt_model.generate_with_history([{"role": "user", "content": message}])
            db_connector.save_conversation(game_idx, 'bot', response)
            print(response)
            return jsonify({"success": True, "response": response})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500
    finally:
        db_connector.close()

@game_chat_bp.route('/history', methods=['GET'])
def get_conversation_history():
    db_connector = GameChatDBConnector()
    try:
        user_id = request.args.get("user_id")
        game_idx = db_connector.select_game_idx(user_id)

        if not game_idx:
            return jsonify({"success": False, "message": "Game not found for the user"}), 400
        # 대화 내역 조회
        query = """
            SELECT SENDER, MESSAGE
            FROM CONVERSATIONS_TB 
            WHERE GAME_IDX = %s
            ORDER BY CREATED_AT ASC
        """
        result = db_connector.select(query, (game_idx,))
        return jsonify({"success": True, "history": result})

    except Exception as error:
        return jsonify({'success': False, 'message': str(error)}), 500
    finally:
        db_connector.close()