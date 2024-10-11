import threading
import logging
from modules.llm.llama import LlamaModel

# 로그 설정
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class SessionManager:
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        self.llama_model = None

    def initialize_model(self):
        if self.llama_model is None:
            self.llama_model = LlamaModel.get_instance()
        logger.debug("LlamaModel initialized.")

    def get_model(self):
        if self.llama_model is None:
            self.initialize_model()
        return self.llama_model

    # def end_session(self, user_id, chatroom_id):
    #     logger.debug(f"Ending session for user_id: {user_id}, chatroom_id: {chatroom_id}")
    #     if self.llama_model:
    #         try:
    #             # logger.debug("Attempting to save model state")
    #             # self.async_save_model_state(user_id, chatroom_id)  # 비동기 저장 호출
    #             # logger.debug("Model state save initiated")
    #             pass
    #         except Exception as e:
    #             logger.error(f"Error saving model state: {str(e)}")
    def delete_session(self, user_id, chatroom_id):
        logger.debug(
            f"Deleting session for user_id: {user_id}, chatroom_id: {chatroom_id}"
        )
        try:
            # delete_model_state(user_id, chatroom_id)
            logger.debug("Model state deleted.")
        except Exception as e:
            logger.error(f"Error deleting model state: {str(e)}")


session_manager = SessionManager.get_instance()  # Global Instance
