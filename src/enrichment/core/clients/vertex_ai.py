# enrichment/core/clients/vertex_ai.py

import logging
from tenacity import retry, wait_exponential_jitter, stop_after_attempt, retry_if_exception_type
from google import genai
from google.genai import types
from .. import config
import google.auth
import google.auth.transport.requests

logging.basicConfig(level=logging.INFO)
_log = logging.getLogger(__name__)

def _init_client() -> genai.Client | None:
    """Initializes the Vertex AI client."""
    try:
        project = config.PROJECT_ID 
        location = "global"

        _log.info(
            "Initializing Vertex GenAI client (project=%s, location=%s)...",
            project, location
        )
        client = genai.Client(
            vertexai=True,
            project=project,
            location=location,
            http_options=types.HttpOptions(api_version="v1"),
        )
        _log.info("Vertex GenAI client initialized successfully.")
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
    before_sleep=lambda rs: _log.warning(
        "Retrying after %s: attempt %d", rs.outcome.exception(), rs.attempt_number
    ),
)
def generate(
    prompt: str,
    *,
    model: str | None = None,
    config_overrides: dict | None = None,
) -> str:
    """Generates content using the Vertex AI client with retry logic."""
    global _client
    if _client is None:
        _log.warning("Vertex client was None; attempting re-init now…")
        _client = _init_client()
        if _client is None:
            raise RuntimeError("Vertex AI client is not available.")

    model_name = model or config.MODEL_NAME
    _log.info(
        "Generating content with Vertex AI (model=%s, prompt_tokens=%d)…",
        model_name,
        len(prompt.split()),
    )

    cfg_kwargs: dict = {
        "temperature": config.TEMPERATURE,
        "top_p": config.TOP_P,
        "top_k": config.TOP_K,
        "seed": config.SEED,
        "candidate_count": config.CANDIDATE_COUNT,
        "max_output_tokens": config.MAX_OUTPUT_TOKENS,
    }
    if config_overrides:
        for key, value in config_overrides.items():
            if value is not None:
                cfg_kwargs[key] = value

    cfg = types.GenerateContentConfig(**cfg_kwargs)

    text = ""
    for chunk in _client.models.generate_content_stream(
        model=model_name,
        contents=prompt,
        config=cfg,
    ):
        if chunk.text:
            text += chunk.text

    _log.info("Successfully received full streamed response from Vertex AI.")
    return text.strip()