"""
Agent Arena — Model API Abstraction Layer
==========================================
Unified interface for 3 providers: xAI/Grok, Google/Gemini, Anthropic/Claude.
All models return structured JSON via their respective APIs.
"""

import os
import json
import logging
import asyncio
import re
from typing import Optional

import anthropic
import openai
from google import genai
from google.genai import types

from config import AGENTS, SYSTEM_PROMPT

logger = logging.getLogger(__name__)


async def call_agent(agent: dict, prompt: str, temperature: float = 0.7) -> str:
    """
    Call a single agent's model API and return the response text.
    Routes to the correct provider based on agent config.
    """
    provider = agent["provider"]
    api_key = os.environ.get(agent["api_key_env"], "")
    
    if not api_key:
        logger.warning(f"No API key for {agent['id']} ({agent['api_key_env']}). Skipping.")
        return json.dumps([])

    try:
        if provider == "anthropic":
            return await _call_anthropic(agent, api_key, prompt, temperature)
        elif provider == "google":
            return await _call_google(agent, api_key, prompt, temperature)
        elif provider in ("openai", "openai_compat"):
            return await _call_openai_compat(agent, api_key, prompt, temperature)
        else:
            logger.error(f"Unknown provider: {provider}")
            return json.dumps([])
    except Exception as e:
        logger.error(f"Error calling {agent['id']}: {e}")
        return json.dumps([])


async def call_all_agents(prompt_template: str, template_kwargs: dict,
                          agents: list = None, temperature: float = 0.7) -> dict:
    """
    Call all agents in parallel with the same prompt.
    Returns dict: {agent_id: response_text}
    """
    if agents is None:
        agents = AGENTS
    
    tasks = []
    for agent in agents:
        prompt = prompt_template.format(**template_kwargs)
        tasks.append(_call_with_id(agent, prompt, temperature))
    
    results = await asyncio.gather(*tasks)
    return {agent_id: response for agent_id, response in results}


async def _call_with_id(agent: dict, prompt: str, temperature: float):
    """Helper to return (agent_id, response) tuple."""
    response = await call_agent(agent, prompt, temperature)
    return (agent["id"], response)


# ============================================================
# Provider-specific implementations
# ============================================================

async def _call_anthropic(agent: dict, api_key: str, prompt: str, temperature: float) -> str:
    """Call Anthropic's Messages API (Claude Sonnet 4)."""
    client = anthropic.AsyncAnthropic(api_key=api_key)
    
    message = await client.messages.create(
        model=agent["model"],
        max_tokens=2048,
        temperature=temperature,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}]
    )
    
    return message.content[0].text


async def _call_google(agent: dict, api_key: str, prompt: str, temperature: float) -> str:
    """Call Google Gemini 3 Flash with thinking_level control."""
    client = genai.Client(api_key=api_key)
    
    thinking_level = agent.get("thinking_level", "high")
    
    response = await asyncio.to_thread(
        client.models.generate_content,
        model=agent["model"],
        contents=f"{SYSTEM_PROMPT}\n\n{prompt}",
        config=types.GenerateContentConfig(
            temperature=temperature,
            max_output_tokens=4096,
            thinking_config=types.ThinkingConfig(
                thinking_budget={"minimal": 128, "low": 1024, "medium": 4096, "high": 8192}.get(thinking_level, 8192)
            ),
        )
    )
    
    # Extract only the text parts, skip thinking parts
    text_parts = []
    if response.candidates:
        for part in response.candidates[0].content.parts:
            # Skip thinking parts — only collect model output
            if hasattr(part, 'thought') and part.thought:
                continue
            if hasattr(part, 'text') and part.text:
                text_parts.append(part.text)
    
    combined = "\n".join(text_parts)
    if not combined.strip():
        # Fallback to .text property
        combined = response.text or "[]"
    
    return combined


async def _call_openai_compat(agent: dict, api_key: str, prompt: str, temperature: float) -> str:
    """
    Call OpenAI-compatible API (xAI/Grok, DeepSeek, OpenAI o3/GPT).
    Handles reasoning models (o3) that don't support temperature/system messages.
    """
    base_url = agent.get("base_url", "https://api.openai.com/v1")
    
    client = openai.AsyncOpenAI(api_key=api_key, base_url=base_url)
    
    model = agent["model"]
    is_reasoning = model.startswith("o1") or model.startswith("o3") or model.startswith("o4")
    
    if is_reasoning:
        # Reasoning models: no temperature, no system role, use max_completion_tokens
        response = await client.chat.completions.create(
            model=model,
            max_completion_tokens=4096,
            messages=[
                {"role": "user", "content": f"{SYSTEM_PROMPT}\n\n{prompt}"},
            ]
        )
    elif model.startswith("gpt-5"):
        # GPT-5.x requires max_completion_tokens instead of max_tokens
        response = await client.chat.completions.create(
            model=model,
            temperature=temperature,
            max_completion_tokens=2048,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ]
        )
    else:
        response = await client.chat.completions.create(
            model=model,
            temperature=temperature,
            max_tokens=2048,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ]
        )
    
    return response.choices[0].message.content


# ============================================================
# Response parsing
# ============================================================

def parse_agent_response(response_text: str) -> list:
    """
    Extract JSON array from agent response.
    Handles markdown code blocks, thinking artifacts, leading text, etc.
    """
    if not response_text:
        return []
    
    text = response_text.strip()
    
    # Strategy 1: Extract from markdown code block
    json_block = _extract_code_block(text)
    if json_block:
        result = _try_parse_json_array(json_block)
        if result is not None:
            return result
    
    # Strategy 2: Find JSON array directly in the text
    result = _extract_json_array(text)
    if result is not None:
        return result
    
    # Strategy 3: Try the whole text
    result = _try_parse_json_array(text)
    if result is not None:
        return result
    
    logger.warning(f"Failed to parse JSON from response: {text[:300]}")
    return []


def _extract_code_block(text: str) -> Optional[str]:
    """Extract content from the first markdown code block."""
    # Match ```json ... ``` or ``` ... ```
    pattern = r'```(?:json)?\s*\n?(.*?)```'
    match = re.search(pattern, text, re.DOTALL)
    if match:
        return match.group(1).strip()
    
    # Handle unclosed code block (Gemini sometimes does this)
    pattern = r'```(?:json)?\s*\n?(.*)'
    match = re.search(pattern, text, re.DOTALL)
    if match:
        return match.group(1).strip()
    
    return None


def _extract_json_array(text: str) -> Optional[list]:
    """Find and extract a JSON array from text using bracket matching."""
    start = text.find("[")
    if start == -1:
        return None
    
    depth = 0
    in_string = False
    escape_next = False
    
    for i in range(start, len(text)):
        c = text[i]
        
        if escape_next:
            escape_next = False
            continue
        
        if c == '\\' and in_string:
            escape_next = True
            continue
        
        if c == '"' and not escape_next:
            in_string = not in_string
            continue
        
        if in_string:
            continue
        
        if c == '[':
            depth += 1
        elif c == ']':
            depth -= 1
            if depth == 0:
                candidate = text[start:i+1]
                return _try_parse_json_array(candidate)
    
    # If we never closed the bracket, try parsing what we have
    candidate = text[start:]
    # Try adding a closing bracket
    return _try_parse_json_array(candidate + "]")


def _try_parse_json_array(text: str) -> Optional[list]:
    """Try to parse text as a JSON array."""
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return parsed
        return [parsed]
    except json.JSONDecodeError:
        return None
