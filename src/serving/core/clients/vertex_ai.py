# /serving/core/clients/vertex_ai.py
import logging

# REMOVED: tenacity imports to prevent auto-retries and hanging
from google import genai
from google.genai import types

from .. import config

logging.basicConfig(level=logging.INFO)
_log = logging.getLogger(__name__)


def _init_client() -> genai.Client | None:
    """Initializes the Vertex AI GenAI client with STRICT FAIL-FAST TIMEOUTS."""
    try:
        project = config.SOURCE_PROJECT_ID  # Serving uses SOURCE_PROJECT_ID
        # Force global for google.genai + Vertex routing (required for preview models)
        location = "global"
        _log.info(
            "Initializing Vertex GenAI client (project=%s, location=%s) with 60s timeout...",
            project,
            location,
        )

        # FAIL FAST CONFIGURATION:
        # 1. timeout=60: Kill connections that hang.
        # 2. api_version="v1beta1": Standard.
        client = genai.Client(
            vertexai=True,
            project=project,
            location=location,
            http_options=types.HttpOptions(
                api_version="v1beta1",
                timeout=60000,  # Timeout in milliseconds (60 seconds)
            ),
        )
        _log.info("Vertex GenAI client initialized successfully.")
        return client
    except Exception as e:
        _log.critical("FAILED to initialize Vertex AI client: %s", e, exc_info=True)
        return None


_client = None


def _get_client() -> genai.Client:
    """Lazy loader for the Vertex AI client."""
    global _client
    if _client is None:
        _client = _init_client()
        if _client is None:
            raise RuntimeError("Vertex AI client is not available.")
    return _client


# REMOVED @retry DECORATOR - WE WANT FAST FAILURES
def generate(prompt: str, response_mime_type: str | None = None) -> str:
    """Generates content using the Vertex AI client (FAIL FAST MODE: No Retries)."""
    client = _get_client()

    _log.info("Generating content (Fail-Fast Mode, model=%s)...", config.MODEL_NAME)
    cfg = types.GenerateContentConfig(
        temperature=config.TEMPERATURE,
        top_p=config.TOP_P,
        top_k=config.TOP_K,
        seed=config.SEED,
        candidate_count=config.CANDIDATE_COUNT,
        max_output_tokens=config.MAX_OUTPUT_TOKENS,
        response_mime_type=response_mime_type,
    )
    text = ""
    # We use stream=True usually, but for fail-fast, generate_content might be safer?
    # Let's stick to stream but wrapped in a try/except at the pipeline level (which is already done).
    # The timeout in _init_client will kill this if it hangs.
    for chunk in client.models.generate_content_stream(
        model=config.MODEL_NAME, contents=prompt, config=cfg
    ):
        if chunk.text:
            text += chunk.text

    return text.strip()
