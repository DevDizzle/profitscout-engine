# transcript_summarizer/core/client.py
import logging
from google import genai
from google.api_core import exceptions as core_exc
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

@retry(
    retry=retry_if_exception_type((core_exc.InternalServerError, core_exc.ServiceUnavailable, core_exc.DeadlineExceeded)),
    wait=wait_exponential(multiplier=2, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
)
def generate_summary_with_retry(model, prompt: str, config: dict) -> str:
    """Generates content with an exponential backoff retry mechanism."""
    response = model.generate_content(prompt, generation_config=config)
    return response.text

class GeminiClient:
    """A client for summarizing text using the Gemini API."""
    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("Gemini API key is required.")
        genai.configure(api_key=api_key)

    def summarize(self, prompt: str, model_name: str, temp: float, max_tokens: int) -> str | None:
        """Generates a summary for a given prompt using the specified model."""
        model = genai.GenerativeModel(model_name)
        config = genai.GenerationConfig(
            temperature=temp,
            max_output_tokens=max_tokens,
        )
        try:
            summary = generate_summary_with_retry(model, prompt, config)
            return summary or "No response from model."
        except Exception as e:
            logging.error(f"Failed to generate summary after retries: {e}")
            return None