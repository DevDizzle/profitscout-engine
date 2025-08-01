import logging
import sys
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from google import genai
from google.genai import types
from . import config

# Basic logging config that runs immediately
logging.basicConfig(level=logging.INFO)
_log = logging.getLogger(__name__)

def _init_client() -> genai.Client | None:
    """
    Initializes and returns the GenAI client.
    Returns None if initialization fails.
    """
    try:
        _log.info("Attempting to initialize Vertex AI GenAI client...")
        client = genai.Client(
            vertexai=True,
            project=config.PROJECT_ID,
            location=config.LOCATION,
            http_options=types.HttpOptions(api_version="v1"),
        )
        _log.info("Vertex AI GenAI client initialized successfully.")
        return client
    except Exception as e:
        # CRITICAL: Log any exception that occurs during initialization
        _log.critical("!!! FAILED to initialize Vertex AI client: %s", e, exc_info=True)
        # Write directly to stderr to ensure it's captured by the logging system
        print(f"CRITICAL: FAILED to initialize Vertex AI client: {e}", file=sys.stderr)
        return None

_client = _init_client()

@retry(
    retry=retry_if_exception_type(Exception),
    wait=wait_exponential(multiplier=2, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
)
def generate(prompt: str) -> str:
    # Add a check to ensure the client was initialized
    if _client is None:
        _log.error("Cannot generate summary because Vertex AI client is not available.")
        # Raise an exception to handle this case upstream
        raise RuntimeError("Vertex AI client failed to initialize.")

    cfg = types.GenerateContentConfig(
        temperature=config.TEMPERATURE,
        max_output_tokens=config.MAX_OUTPUT_TOKENS,
        candidate_count=config.CANDIDATE_COUNT,
    )
    resp = _client.models.generate_content(
        model=config.MODEL_NAME,
        contents=prompt,
        config=cfg,
    )
    return resp.text.strip()