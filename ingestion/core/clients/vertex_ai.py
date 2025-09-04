import logging
from tenacity import retry, wait_exponential_jitter, stop_after_attempt, retry_if_exception_type
from google import genai
from google.genai import types
from .. import config
import time

logging.basicConfig(level=logging.INFO)
_log = logging.getLogger(__name__)

def _init_client() -> genai.Client | None:
    """Initializes the Vertex AI client without an explicit read timeout."""
    try:
        loc = "global"  # Force global for better reliability
        _log.info("Initializing Vertex AI GenAI client with project=%s, location=%s...", config.PROJECT_ID, loc)
        client = genai.Client(
            vertexai=True,
            project=config.PROJECT_ID,
            location=loc,
            # Keep API on v1 and omit timeout entirely
            http_options=types.HttpOptions(api_version="v1"),
        )
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
    if _client is None:
        raise RuntimeError("Vertex AI client is not available.")

    # Log prompt size for debugging
    _log.info("Generating content with Vertex AI (model=%s, prompt_tokens=%d)...",
              config.MODEL_NAME, len(prompt.split()))

    cfg = types.GenerateContentConfig(
        temperature=config.TEMPERATURE,
        top_p=config.TOP_P,
        top_k=config.TOP_K,
        seed=config.SEED,
        candidate_count=config.CANDIDATE_COUNT,
        max_output_tokens=config.MAX_OUTPUT_TOKENS,
    )
    
    text = ""
    for chunk in _client.models.generate_content_stream(
        model=config.MODEL_NAME,
        contents=prompt,
        config=cfg,
    ):
        if chunk.text:
            text += chunk.text

    _log.info("Successfully received full streamed response from Vertex AI.")
    return text.strip()
