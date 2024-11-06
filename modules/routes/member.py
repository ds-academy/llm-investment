import json
from flask import Blueprint, request, jsonify, current_app
from modules.db.member_db import MemberDBConnector
from flask_bcrypt import Bcrypt

member_bp = Blueprint("members", __name__)
bcrypt = Bcrypt()

@member_bp.route('/test', methods=['GET'])
def test():
    print("hello")

@member_bp.route('/join', methods=['POST'])
def join():
    db_connector = MemberDBConnector()
    try:
        # 데이터 수신
        user_data = {
            "user_id": request.json.get("id"),
            "user_pw": bcrypt.generate_password_hash(request.json.get("pw")).decode('utf-8'),
            "user_nickname": request.json.get("nickName"),
            "user_name": request.json.get("name"),
            "user_phone": request.json.get("phoneNumber"),
            "user_email": request.json.get("email"),
            "join_date": request.json.get("joinDate")
        }

        # 데이터베이스에 사용자 추가
        db_connector.insert_user(user_data)
        return jsonify({"success": True, "message": "User registered successfully"})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

    finally:
        db_connector.close()

@member_bp.route('/login', methods=['POST'])
def login():
    db_connector = MemberDBConnector()
    try:
        user_id = request.json.get("id")
        pw = request.json.get("pw")

        # 사용자 정보 가져오기
        user = db_connector.get_user_by_id(user_id)
        if user and bcrypt.check_password_hash(user["user_pw"], pw):
            print(user)
            return jsonify({"success": True, "token": user})
        else:
            return jsonify({"success": False, "message": "Invalid credentials"})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

    finally:
        db_connector.close()
