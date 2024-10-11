from openai import OpenAI
from typing import List, Dict, Any, Tuple
from modules.llm.utils import GPT_PROMPT


class GPTModel:
    def __init__(self, api_key: str, model_id: str = "gpt-3.5-turbo"):
        """
        GPTModel 클래스 초기화
        :param api_key: OpenAI API 키
        :param model_id: 사용할 GPT 모델 ID (기본값: gpt-3.5-turbo)
        """
        self.client = OpenAI(api_key=api_key)
        self.model_id = model_id

    def generate(self, instruction: str) -> str:
        """
        주어진 지시에 따라 텍스트 생성
        :param instruction: 사용자의 입력 지시
        :return: 생성된 텍스트
        """
        return self._create_chat_completion(self.model_id, instruction)

    def fine_tune(self, file_path: str) -> Tuple[str, str]:
        """
        모델 fine-tuning 작업 시작
        :param file_path: 훈련 데이터 파일 경로
        :return: fine-tuning 작업 ID와 상태
        """
        try:
            with open(file_path, "rb") as file:
                uploaded_file = self.client.files.create(file=file, purpose="fine-tune")
            job = self.client.fine_tuning.jobs.create(
                training_file=uploaded_file.id,
                model=self.model_id
            )
            return job.id, job.status
        except Exception as e:
            raise RuntimeError(f"Fine-tuning 오류: {str(e)}")

    def get_fine_tune_status(self, job_id: str) -> Dict[str, Any]:
        """
        fine-tuning 작업의 상태 확인
        :param job_id: fine-tuning 작업 ID
        :return: 작업 상태 정보
        """
        try:
            job = self.client.fine_tuning.jobs.retrieve(job_id)
            return {
                'job_id': job.id,
                'status': job.status,
                'fine_tuned_model': job.fine_tuned_model
            }
        except Exception as e:
            raise RuntimeError(f"상태 확인 오류: {str(e)}")

    def list_fine_tune_jobs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Fine-tuning 작업 목록 조회
        :param limit: 조회할 작업 수
        :return: Fine-tuning 작업 목록
        """
        try:
            jobs = self.client.fine_tuning.jobs.list(limit=limit)
            return [{"id": job.id, "status": job.status} for job in jobs.data]
        except Exception as e:
            raise RuntimeError(f"Fine-tuning 작업 목록 조회 오류: {str(e)}")

    def list_fine_tuned_models(self) -> List[Dict[str, Any]]:
        """
        Fine-tuned 모델 목록 조회
        :return: Fine-tuned 모델 목록
        """
        try:
            models = self.client.models.list()
            return [model.model_dump() for model in models.data if model.id.startswith("ft:")]
        except Exception as e:
            raise RuntimeError(f"Fine-tuned 모델 목록 조회 오류: {str(e)}")

    def generate_with_history(self, messages: List[Dict[str, str]]) -> str:
        try:
            response = self.client.chat.completions.create(
                model=self.model_id,
                messages=[{"role": "system", "content": GPT_PROMPT}] + messages
            )
            return response.choices[0].message.content
        except Exception as e:
            raise RuntimeError(f"텍스트 생성 오류: {str(e)}")

    def generate_with_fine_tuned_model(self, instruction: str, fine_tuned_model_id: str) -> str:
        """
        Fine-tuned 모델을 사용하여 텍스트 생성
        :param instruction: 사용자의 입력 지시
        :param fine_tuned_model_id: Fine-tuned 모델의 ID
        :return: 생성된 텍스트
        """
        return self._create_chat_completion(fine_tuned_model_id, instruction)

    def get_model_info(self, model_id: str) -> Dict[str, Any]:
        """
        모델 정보 조회
        :param model_id: 모델 ID
        :return: 모델 정보
        """
        try:
            model = self.client.models.retrieve(model_id)
            return model.model_dump()
        except Exception as e:
            raise RuntimeError(f"모델 정보 조회 오류: {str(e)}")

    def _create_chat_completion(self, model_id: str, instruction: str) -> str:
        """
        ChatCompletion API를 사용하여 텍스트 생성
        :param model_id: 사용할 모델 ID
        :param instruction: 사용자의 입력 지시
        :return: 생성된 텍스트
        """
        try:
            response = self.client.chat.completions.create(
                model=model_id,
                messages=[
                    {"role": "system", "content": GPT_PROMPT},
                    {"role": "user", "content": instruction}
                ]
            )
            return response.choices[0].message.content
        except Exception as e:
            raise RuntimeError(f"텍스트 생성 오류: {str(e)}")

# if __name__ == "__main__":
#     api_key = os.environ.get("OPENAI_API_KEY")
#     if not api_key:
#         raise ValueError("OPENAI_API_KEY 환경 변수가 설정되지 않았습니다.")
#
#     gpt_model = GPTModel(api_key=api_key, model_id="gpt-3.5-turbo-0125")
#     fine_tune_jobs = gpt_model.list_fine_tune_jobs(20)
#     for job in fine_tune_jobs:
#         print(f"Job ID: {job['id']}, Status: {job['status']}")

