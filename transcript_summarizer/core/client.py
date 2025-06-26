import logging
from google import genai
from google.genai.types import GenerateContentConfig
from google.api_core import exceptions as core_exc
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from config import MODEL_NAME, TEMPERATURE, MAX_TOKENS

class GeminiClient:
    """A client for summarizing text using the Gemini API."""
    def __init__(self, api_key: str):
        self.client = genai.Client(api_key=api_key)
        logging.info(f"GeminiClient initialized for model: {MODEL_NAME}")

    @retry(
        retry=retry_if_exception_type((core_exc.InternalServerError, core_exc.ServiceUnavailable)),
        wait=wait_exponential(multiplier=1, max=60),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def generate_with_retry(self, prompt: str) -> str:
        """Makes a retriable call to the Gemini API."""
        response = self.client.models.generate_content(
            model=MODEL_NAME,
            contents=prompt,
            config=GenerateContentConfig(
                temperature=TEMPERATURE,
                max_output_tokens=MAX_TOKENS,
            ),
        )
        return response.text or "No response"

    def summarize(self, prompt: str) -> str | None:
        """Generates a summary using the Gemini API."""
        try:
            summary = self.generate_with_retry(prompt)
            return summary
        except Exception as e:
            logging.error(f"Failed to generate summary after all retries: {e}", exc_info=True)
            return None