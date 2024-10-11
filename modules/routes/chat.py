import json
from flask import Blueprint, request, jsonify, current_app
from modules.db.chat_db import ChatDBConnector
from modules.routes.session_manager import SessionManager
from modules.llm.chat_gpt import GPTModel
from modules.llm.utils import format_recent_chat_history

chat_bp = Blueprint("chat", __name__)
# 키워드를 변수로 저장
KEYWORDS = ["죄송합니다", "모르겠습니다", "잘 모르겠", "gpt help"]
session_manager = SessionManager.get_instance()


def get_gpt_model():
    api_key = current_app.config["OPENAI_API_KEY"]
    model_id = current_app.config["GPT_MODEL_ID"]
    return GPTModel(api_key=api_key, model_id=model_id)


def evaluate_response(response: str, keywords: list) -> bool:
    """응답에 특정 키워드가 포함되어 있는지 확인"""
    return any(keyword in response for keyword in keywords)


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


@chat_bp.route("/ask", methods=["POST"])
def ask_question():
    try:
        data = request.get_json()
        question = data.get("question")
        user_id = data.get("user_id")
        room_id = data.get("room_id")
        if not all([question, user_id, room_id]):
            missing = []
            if not question:
                missing.append("질문")
            if not user_id:
                missing.append("유저 ID")
            if not room_id:
                missing.append("Room ID")
            return (
                jsonify(
                    {"error": f"다음 정보가 제공되지 않았습니다: {', '.join(missing)}"}
                ),
                400,
            )

        # Question 자체에 대한 필터링 필요함. 빈 값이거나 특정 키워드가 포함되어 있으면 에러 반환
        if not question.strip():
            return jsonify({"error": "질문이 비어 있습니다."}), 400

        # Llama model response
        llama_model = session_manager.get_model()
        db_connector = ChatDBConnector()
        chat_history = db_connector.get_chat_history(room_id)  # Return: str

        if chat_history is None or len(chat_history) == 0:
            # 새로운 대화
            model_response = llama_model.generate_response(question)
        else:
            # 기존 재화 존재시
            chat_history = format_recent_chat_history(chat_history, n=3)

            model_response = llama_model.generate_response_with_history(
                question, chat_history
            )

        try:
            model_response, error = parse_model_response(model_response)
            if error:
                # 로깅
                current_app.logger.error(f"Model response parsing error: {error}")

                # 기본 응답 생성
                model_response = {
                    "answer": "죄송합니다. 응답을 처리하는 중 오류가 발생했습니다.",
                    "user_invest_type": None,
                    "confidence": 0
                }

            model_answer = model_response["answer"]
            user_invest_type = model_response["user_invest_type"]
            answer_confidence = model_response["confidence"]

        except Exception as e:
            current_app.logger.error(f"Unexpected error in parse_model_response: {str(e)}")
            return jsonify({"error": "내부 서버 오류가 발생했습니다."}), 500

        use_gpt = evaluate_response(model_answer, KEYWORDS) or answer_confidence < 0.4

        if use_gpt:
            # GPT Model
            gpt_model = get_gpt_model()
            if len(chat_history) > 0:
                # 기존 대화 존재시, 현재 대화까지 붙여서 같이 보냄
                chat_history.extend([{"role": "user", "content": question}])
                gpt_response = gpt_model.generate_with_history(chat_history)
            else:
                # 신규 대화일 경우?
                gpt_response = gpt_model.generate(question)

            gpt_response, error = parse_model_response(gpt_response)

            if error:
                current_app.logger.error(f"GPT response parsing error: {error}")
                # 이전 LLAMA 모델의 응답을 유지
            else:
                model_answer = gpt_response["answer"]
                user_invest_type = gpt_response["user_invest_type"]
                answer_confidence = gpt_response["confidence"]

            # 각각 값 없는 경우, 이전 LLAMA 모델 활용
            model_answer = gpt_response.get("answer", model_answer)
            user_invest_type = gpt_response.get("user_invest_type", user_invest_type)
            answer_confidence = gpt_response.get("confidence", answer_confidence)

        db_connector.save_chat_history(room_id, "user", question)
        db_connector.save_chat_history(
            room_id, "gpt" if use_gpt else "llama", model_answer
        )
        response = {
            "response": model_answer,
            "chatroom_id": room_id,
            "user_invest_type": user_invest_type,
            "confidence": answer_confidence,
        }
        return jsonify(response)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db_connector.close()


@chat_bp.route("/delete-chatroom", methods=["POST"])
def delete_chatroom():
    try:
        data = request.get_json()
        chatroom_id = data.get("chatroom_id")
        user_id = data.get("user_id")
        if not chatroom_id or not user_id:
            return jsonify({"error": "채팅방 ID와 유저 ID가 제공되지 않았습니다."}), 400
        db_connector = ChatDBConnector()
        try:
            db_connector.delete_chatroom(chatroom_id)
            # session_manager.delete_session(user_id, chatroom_id)
            return jsonify({"message": "채팅방 삭제 완료", "chatroom_id": chatroom_id})
        except Exception as e:
            return jsonify({"error": f"채팅방 삭제 중 오류 발생: {str(e)}"}), 500
        finally:
            db_connector.close()
    except Exception as e:
        return jsonify({"error": f"서버 오류 발생: {str(e)}"}), 500


@chat_bp.route("/history", methods=["GET"])
def get_history():
    chatroom_id = request.args.get("chatroom_id")
    if not chatroom_id:
        return jsonify({"error": "채팅방 ID가 제공되지 않았습니다."}), 400
    db_connector = ChatDBConnector()
    try:
        chat_history = db_connector.get_chat_history(chatroom_id)
        return jsonify({"chat_history": chat_history})
    finally:
        db_connector.close()


@chat_bp.route("/create-room", methods=["POST"])
def create_room():
    try:
        data = request.get_json()
        user_id = data.get("user_id")
        if not user_id:
            return jsonify({"error": "user_id 제공되지 않음"}), 400
        db_connector = ChatDBConnector()
        try:
            chatroom_count = db_connector.get_chatroom_count_by_userid(user_id)
            if chatroom_count >= 3:
                return (
                    jsonify({"error": "최대 3개의 채팅방만 생성할 수 있습니다."}),
                    400,
                )
            room_id = db_connector.create_chatroom(user_id)
            if not room_id:
                raise Exception("Chat room creation failed")
            return jsonify({"room_id": room_id})
        finally:
            db_connector.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def init_chat_module(app):
    app.register_blueprint(chat_bp)
    if "OPENAI_API_KEY" not in app.config:
        raise ValueError("OPENAI_API_KEY 환경 변수가 설정되지 않았습니다.")
