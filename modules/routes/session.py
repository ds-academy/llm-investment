from flask import Blueprint, request, jsonify, current_app
from modules.db.db_connector import DBConnector
from modules.routes.session_manager import SessionManager

session_bp = Blueprint("session", __name__)
session_manager = SessionManager.get_instance()


@session_bp.route("/init-session", methods=["POST"])
def init_session():
    data = request.json
    userid = data.get("userid")
    investtype = data.get("investtype")
    if not userid:
        return jsonify({"error": "유저 ID가 제공되지 않았습니다."}), 400
    db_connector = DBConnector()
    try:
        # 유저의 채팅룸 개수 확인
        chatroom_count = db_connector.get_chatroom_count_by_userid(userid)
        if chatroom_count >= 3:
            return (
                jsonify({"error": "한 유저당 최대 3개의 채팅룸만 생성할 수 있습니다."}),
                400,
            )
        # userid로 가장 최근에 대화한 채팅방 조회
        chatroom_id = db_connector.get_last_active_chatroom_by_userid(userid)
        if not chatroom_id:
            # 기존 채팅방이 없으면 생성
            chatroom_id = db_connector.create_chatroom(userid)
        # 모델 초기화
        session_manager.initialize_model()
    finally:
        db_connector.close()
    return jsonify(
        {
            "message": "세션 초기화 완료",
            "chatroom_id": chatroom_id,
            "userid": userid,
            "investtype": investtype,
        }
    )


@session_bp.route("/end-session", methods=["POST"])
def end_session_route():
    data = request.json
    user_id = data.get("user_id")
    chatroom_id = data.get("chatroom_id")
    if not user_id or not chatroom_id:
        return jsonify({"error": "사용자 ID와 채팅방 ID가 제공되지 않았습니다."}), 400
    session_manager.end_session(user_id, chatroom_id)
    return jsonify(
        {
            "message": "세션 종료 및 모델 상태 저장 완료",
            "user_id": user_id,
            "chatroom_id": chatroom_id,
        }
    )


@session_bp.route("/logout", methods=["POST"])
def logout():
    data = request.json
    user_id = data.get("userId")
    if not user_id:
        return jsonify({"error": "사용자 ID가 제공되지 않았습니다."}), 400
    # 사용자의 모든 세션 종료 및 모델 상태 저장
    db_connector = DBConnector()
    try:
        chatrooms = db_connector.get_user_chatrooms(user_id)
        for chatroom in chatrooms:
            chatroom_id = chatroom["chatroom_id"]
            session_manager.end_session(user_id, chatroom_id)
    finally:
        db_connector.close()
    return jsonify({"message": "로그아웃 및 모든 세션 종료 완료", "user_id": user_id})
