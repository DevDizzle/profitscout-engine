import logging
import sys
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from google import genai
from google.genai import types
from . import config

logging.basicConfig(level=logging.INFO)
_log = logging.getLogger(__name__)

def _init_client() -> genai.Client | None:
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
        _log.critical("!!! FAILED to initialize Vertex AI client: %s", e, exc_info=True)
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
    if _client is None:
        _log.error("Cannot generate summary because Vertex AI client is not available.")
        raise RuntimeError("Vertex AI client failed to initialize.")

    cfg = types.GenerateContentConfig(
        temperature=config.TEMPERATURE,
        top_p=config.TOP_P,
        top_k=config.TOP_K,
        seed=config.SEED,
        candidate_count=config.CANDIDATE_COUNT,
        max_output_tokens=config.MAX_OUTPUT_TOKENS,
    )

    resp = _client.models.generate_content(
        model=config.MODEL_NAME,
        contents=prompt,
        config=cfg,
    )
    return resp.text.strip()
