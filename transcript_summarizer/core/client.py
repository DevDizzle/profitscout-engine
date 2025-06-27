# transcript_summarizer/core/client.py
import logging
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from google import genai
from google.genai import types
from . import config

logging.basicConfig(level=logging.INFO)
_log = logging.getLogger(__name__)

def _init() -> genai.Client:
    _log.info("Initialising Vertex AI GenAI client …")
    return genai.Client(
        vertexai=True,
        project=config.PROJECT_ID,
        location=config.LOCATION,
        http_options=types.HttpOptions(api_version="v1"),
    )

_client = _init()

@retry(
    retry=retry_if_exception_type(Exception),
    wait=wait_exponential(multiplier=2, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
)
def generate(prompt: str) -> str:
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
