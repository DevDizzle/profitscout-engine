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
    """Initializes the Vertex AI GenAI client."""
    try:
        project = config.PROJECT_ID
        location = getattr(config, "LOCATION", "global")
        _log.info("Initializing Vertex GenAI client (project=%s, location=%s)…", project, location)
        client = genai.Client(vertexai=True, project=project, location=location, http_options=types.HttpOptions(api_version="v1"))
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


@retry(
    retry=retry_if_exception_type(Exception),
    wait=wait_exponential_jitter(initial=2, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
    before_sleep=lambda rs: _log.warning("Retrying stream after %s: attempt %d", rs.outcome.exception(), rs.attempt_number),
)
def generate(prompt: str) -> str:
    """Generates content using the Vertex AI client with retry logic (streaming)."""
    client = _get_client()

    _log.info("Generating content with Vertex AI (model=%s, prompt_tokens=%d)…", config.MODEL_NAME, len(prompt.split()))
    cfg = types.GenerateContentConfig(
        temperature=config.TEMPERATURE, top_p=config.TOP_P, top_k=config.TOP_K,
        seed=config.SEED, candidate_count=config.CANDIDATE_COUNT, max_output_tokens=config.MAX_OUTPUT_TOKENS,
    )
    text = ""
    for chunk in client.models.generate_content_stream(model=config.MODEL_NAME, contents=prompt, config=cfg):
        if chunk.text:
            text += chunk.text
    _log.info("Successfully received full streamed response from Vertex AI.")
    return text.strip()


@retry(
    retry=retry_if_exception_type(Exception),
    wait=wait_exponential_jitter(initial=2, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
    before_sleep=lambda rs: _log.warning("Retrying tool call after %s: attempt %d", rs.outcome.exception(), rs.attempt_number),
)
def generate_with_tools(
    prompt: str,
    model_name: str | None = None,
    temperature: float | None = None
) -> tuple[str, types.GroundingMetadata | None]:
    """
    Generate a response using Gemini with web-access tools (Search and Browse).

    This function is a general-purpose replacement for the old `generate_grounded_json`.
    It enables the necessary tools for the model to perform both Google searches and
    browse specific URLs mentioned in the prompt.
    """
    client = _get_client()

    # Use pipeline-specific model/temp, or fall back to global defaults
    effective_model = model_name or config.MODEL_NAME
    effective_temp = temperature if temperature is not None else config.TEMPERATURE

    _log.info(
        "Generating with tools on Vertex AI (model=%s, temp=%.2f, prompt_tokens=%d)…",
        effective_model, effective_temp, len(prompt.split())
    )

    # Enable Google Search grounding tool. For modern Gemini models, this single
    # tool provides the capability for both general web search and for browsing
    # specific URLs found within the prompt.
    google_search_tool = types.Tool(google_search=types.GoogleSearch())

    cfg = types.GenerateContentConfig(
        temperature=effective_temp,
        top_p=config.TOP_P, top_k=config.TOP_K, seed=config.SEED,
        candidate_count=1, max_output_tokens=config.MAX_OUTPUT_TOKENS,
        # IMPORTANT: Provide the tool to the model
        tools=[google_search_tool],
    )

    response = client.models.generate_content(
        model=effective_model, contents=prompt, config=cfg
    )

    text = response.text or ""
    grounding_md = None
    try:
        if response.candidates:
            candidate = response.candidates[0]
            grounding_md = getattr(candidate, "grounding_metadata", None) or getattr(candidate, "groundingMetadata", None)
    except Exception as e:
        _log.warning("Failed to read grounding_metadata from response: %s", e, exc_info=True)

    _log.info("Successfully received tool-enabled response from Vertex AI.")
    return text.strip(), grounding_md

# --- Important Final Step ---
# You will now need to update your other pipelines (`macro_thesis.py` and `news_analyzer.py`)
# to call this new `generate_with_tools` function instead of the old one.

# In `macro_thesis.py`, change:
# response_text, _ = vertex_ai.generate_grounded_json(WORLDVIEW_PROMPT)
# TO:
# response_text, _ = vertex_ai.generate_with_tools(
#     prompt=WORLDVIEW_PROMPT,
#     model_name=getattr(config, "MACRO_THESIS_MODEL_NAME", config.MODEL_NAME),
#     temperature=getattr(config, "MACRO_THESIS_TEMPERATURE", config.TEMPERATURE)
# )

# In `news_analyzer.py`, change:
# response_text, _ = vertex_ai.generate_grounded_json(...)
# TO:
# response_text, _ = vertex_ai.generate_with_tools(
#     prompt=prompt,
#     model_name=getattr(config, "NEWS_ANALYZER_MODEL_NAME", config.MODEL_NAME),
#     temperature=getattr(config, "NEWS_ANALYZER_TEMPERATURE", config.TEMPERATURE)
# )