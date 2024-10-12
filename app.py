import os
from dotenv import load_dotenv
from flask import Flask
from flask_cors import CORS
from modules.routes.chat import chat_bp
from modules.routes.session import session_bp

# .env file load (You must have .env file in the root directory)
load_dotenv()

app = Flask(__name__)
CORS(app, supports_credentials=True)

app.config["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")
app.config["GPT_MODEL_ID"] = os.getenv("GPT_MODEL_ID", "ft:gpt-3.5-turbo-0125:personal:llm-experiment:9oAnm4r1")
app.config["MODEL_ID"] = os.getenv("MODEL_ID", "MLP-KTLim/llama-3-Korean-Bllossom-8B")
app.config["MODEL_PATH"] = os.getenv("MODEL_PATH", "llama3/ft2_invest")
app.config["DEBUG"] = os.getenv("DEBUG", "False").lower() == "true"
app.config["PORT"] = int(os.getenv("PORT", 30000))

# Add blueprints
app.register_blueprint(chat_bp)
app.register_blueprint(session_bp)

if __name__ == "__main__":
    if not app.config["OPENAI_API_KEY"]:
        raise ValueError("OPENAI_API_KEY not set")

    app.run(host="0.0.0.0", port=app.config["PORT"], debug=app.config["DEBUG"])