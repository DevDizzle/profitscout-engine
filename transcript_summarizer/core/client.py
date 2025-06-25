import logging
from google import genai
from google.genai import types  # Import types for GenerationConfig
from google.api_core import exceptions as core_exc
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

@retry(
    retry=retry_if_exception_type((core_exc.InternalServerError, core_exc.ServiceUnavailable, core_exc.DeadlineExceeded)),
    wait=wait_exponential(multiplier=2, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
)
def generate_summary_with_retry(model, prompt: str, config: types.GenerationConfig) -> str:
    """Generates content with an exponential backoff retry mechanism."""
    response = model.generate_content(prompt, generation_config=config)
    return response.text

class GeminiClient:
    """A client for summarizing text using the Gemini API via ADC."""
    def __init__(self, model_name: str):
        # Initialize the model directly. This is the correct pattern.
        # The library will automatically use Application Default Credentials.
        self.model = genai.GenerativeModel(model_name)

    def summarize(self, prompt: str, temp: float, max_tokens: int) -> str | None:
        """Generates a summary for a given prompt using the specified model."""
        config = types.GenerationConfig(  # Use types.GenerationConfig
            temperature=temp,
            max_output_tokens=max_tokens,
        )
        try:
            # Pass the initialized model to the retry function
            summary = generate_summary_with_retry(self.model, prompt, config)
            return summary or "No response from model."
        except Exception as e:
            # Catch the AttributeError specifically if it occurs
            if "has no attribute 'GenerationConfig'" in str(e) or "has no attribute 'GenerativeModel'" in str(e):
                logging.error(f"FATAL: The 'google-genai' library version is incompatible or not installed correctly. {e}", exc_info=True)
            else:
                logging.error(f"Failed to generate summary after retries: {e}", exc_info=True)
            return None