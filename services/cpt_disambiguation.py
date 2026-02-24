"""CPT disambiguation service using Claude Haiku for reranking FTS results.

Falls back gracefully when ANTHROPIC_API_KEY is missing or any error occurs.
"""

import json
import os


async def disambiguate_cpt_results(
    user_query: str, fts_results: list[dict], max_results: int = 10
) -> tuple[list[dict], bool]:
    """Rerank FTS results by clinical relevance using Claude Haiku.

    Args:
        user_query: The original search query from the user.
        fts_results: List of dicts with at least 'code' and 'description' keys.
        max_results: Maximum number of results to return.

    Returns:
        Tuple of (reranked results, disambiguation_used).
        If Haiku is unavailable, returns (original results[:max_results], False).
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return fts_results[:max_results], False

    try:
        import anthropic

        client = anthropic.AsyncAnthropic(api_key=api_key)

        codes_text = "\n".join(
            f"- {r['code']}: {r['description']}" for r in fts_results
        )

        message = await client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=512,
            messages=[
                {
                    "role": "user",
                    "content": (
                        f'A user searched for: "{user_query}"\n\n'
                        f"Here are CPT code matches:\n{codes_text}\n\n"
                        "Return a JSON array of CPT codes ranked by clinical relevance "
                        "to the user's query, most relevant first. "
                        'Return ONLY the JSON array, e.g. ["27447", "27130"]'
                    ),
                }
            ],
        )

        ranked_codes = json.loads(message.content[0].text)
        if not isinstance(ranked_codes, list):
            return fts_results[:max_results], False

        # Rebuild result list in ranked order
        result_map = {r["code"]: r for r in fts_results}
        ranked_results = []
        for code in ranked_codes:
            if code in result_map:
                ranked_results.append(result_map[code])
        # Append any codes Haiku missed
        for r in fts_results:
            if r["code"] not in {rr["code"] for rr in ranked_results}:
                ranked_results.append(r)

        return ranked_results[:max_results], True

    except Exception:
        return fts_results[:max_results], False
