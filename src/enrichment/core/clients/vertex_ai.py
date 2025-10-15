import logging
from tenacity import retry, wait_exponential_jitter, stop_after_attempt, retry_if_exception_type
import google.generativeai as genai
from google.generativeai import types
from .. import config
import time

logging.basicConfig(level=logging.INFO)
_log = logging.getLogger(__name__)

def _init_client() -> genai.GenerativeModel | None:
    """Initializes the Vertex AI client without an explicit read timeout."""
    try:
        loc = "global"  # Force global for better reliability
        _log.info("Initializing Vertex AI GenAI client with project=%s, location=%s...", config.PROJECT_ID, loc)
        client = genai.GenerativeModel(config.MODEL_NAME)
        _log.info("Vertex AI GenAI client initialized successfully.")
        return client
    except Exception as e:
        _log.critical("FAILED to initialize Vertex AI client: %s", e, exc_info=True)
        return None

_client = _init_client()

@retry(
    retry=retry_if_exception_type(Exception),
    wait=wait_exponential_jitter(initial=2, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
    before_sleep=lambda rs: _log.warning("Retrying after %s: attempt %d", rs.outcome.exception(), rs.attempt_number),
)
def generate(prompt: str) -> str:
    """Generates content using the Vertex AI client with retry logic."""
    global _client
    if _client is None:
        _log.warning("Vertex client was None; attempting re-init now…")
        _client = _init_client()
        if _client is None:
            raise RuntimeError("Vertex AI client is not available.")

    # Log prompt size for debugging
    _log.info("Generating content with Vertex AI (model=%s, prompt_tokens=%d)...",
              config.MODEL_NAME, len(prompt.split()))

    cfg = types.GenerationConfig(
        temperature=config.TEMPERATURE,
        top_p=config.TOP_P,
        top_k=config.TOP_K,
        candidate_count=config.CANDIDATE_COUNT,
        max_output_tokens=config.MAX_OUTPUT_TOKENS,
    )
    
    response = _client.generate_content(prompt, generation_config=cfg)

    _log.info("Successfully received response from Vertex AI.")
    return response.text.strip()
