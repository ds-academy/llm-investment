import json
from flask import Blueprint, request, jsonify, current_app
from modules.db.member_db import MemberDBConnector
from flask_bcrypt import Bcrypt
from werkzeug.utils import secure_filename
import os
import uuid

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
            return jsonify({"success": True, "token": user["user_id"]})
        else:
            return jsonify({"success": False, "message": "Invalid credentials"})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

    finally:
        db_connector.close()

@member_bp.route('/my_profile', methods=['POST'])
def my_profile():
    db_connector = MemberDBConnector()

    # 요청에서 사용자 토큰 가져오기
    user_id = request.json.get("token")
    print(user_id)
    
    try:
        if not user_id:
            return jsonify({"success": False, "message": "Token is missing"}), 400

        # DB에서 유저 정보 조회
        user_info = db_connector.select_my_profile(user_id)

        if user_info:
            return jsonify({
                "success": True,
                "user_info": {
                    "nickname": user_info['USER_NICKNAME'],
                    "email": user_info['USER_EMAIL'],
                    "profile": f"{request.host_url}{user_info['USER_PROFILE']}",
                    "rank": user_info['user_rank'],
                    "win_count": user_info['WIN_COUNT'],
                    "lose_count": user_info['LOSE_COUNT'],
                    "points": user_info['USER_SCORE'],
                    "name": user_info['USER_NAME']
                }
            })
        else:
            return jsonify({"success": False, "message": "User not found"}), 404
    except Exception as e:
        print(f"Error in /my_profile route: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        db_connector.close()

ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}
UPLOAD_FOLDER = 'static/uploads/profile_images'  # 이미지 저장 경로 설정

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@member_bp.route("/upload_profile_image", methods=["POST"])
def upload_profile_image():
    db_connector = MemberDBConnector()

    try:
        # 파일 유무 확인
        if 'profile_image' not in request.files:
            return jsonify({"success": False, "message": "No file part"})

        file = request.files['profile_image']
        print("프로필 업로드 파일 이미지 확인 : ", file)

        if file.filename == '':
            return jsonify({"success": False, "message": "No selected file"})

        if file and allowed_file(file.filename):
            # 안전한 파일 이름 사용
            ext = file.filename.rsplit('.', 1)[1].lower()
            unique_filename = f"{uuid.uuid4().hex}.{ext}"
            filepath = os.path.join(UPLOAD_FOLDER, unique_filename)
            os.makedirs(os.path.dirname(filepath), exist_ok=True)  # 폴더가 없으면 생성
            file.save(filepath)

            # 서버에 저장된 파일의 URL 반환
            profile_image_url = f"{UPLOAD_FOLDER}/{unique_filename}"

            # DB에 이미지 URL 저장
            user_id = request.form.get('user_id')  # 클라이언트에서 보낸 user_id
            if not user_id:
                # user_id가 없으면 token으로 조회
                token = request.form.get('token')
                user_id = db_connector.get_user_id_from_token(token) if token else None
            
            if not user_id:
                return jsonify({"success": False, "message": "User ID is missing or invalid"})

            print("프로필 업로드 유저 아이디 : ", user_id)
            print("프로필 업로드 사진", profile_image_url)
            db_connector.update_profile_image(user_id, profile_image_url)
            profile_image_url = f"{request.host_url}{profile_image_url}"
            return jsonify({"success": True, "profile_image_url": profile_image_url, "message": "Profile image uploaded successfully"})

        else:
            return jsonify({"success": False, "message": "File type not allowed"})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

    finally:
        db_connector.close()

# memebr.py
@member_bp.route('/rank_list', methods=['POST'])
def rank_list():
    db_connector = MemberDBConnector()
    try:
        # 사용자 랭킹 리스트 가져오기
        rankList = db_connector.get_user_rank_list()
        return jsonify({'success':True, 'rankList':rankList})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

    finally:
        db_connector.close()

# member.py
@member_bp.route('/tip_stock_wise_words', methods=['POST'])
def tip_stock_wise_words():
    db_connector = MemberDBConnector()
    try:
        # 금융단어 목록 가져오기
        stockWords = db_connector.get_stock_words()
        stockTips = db_connector.get_stock_tips()
        wiseSays = db_connector.get_wise_says()
        return jsonify({
            'success': True, 
            'stockWords': stockWords,
            'stockTips': stockTips,
            'wiseSays': wiseSays,
            })
    
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})