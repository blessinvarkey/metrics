# backend/app/services/evals.py

import os
import asyncio
import pandas as pd

# Import the full pipeline entry-point and context fetcher
from api.queries import main as handle_query, fetch_context
from models.entities import UserQuestionRequest

# Hard-coded question for initial testing
def get_test_questions():
    # Replace this list with your own test cases as needed
    return [
        "Show me total sales by region for the last quarter."
    ]

async def _evaluate_batch(questions):
    """
    Call the full production pipeline (api.queries.main) for each question.
    """
    results = []

    # Minimal auth dict to satisfy Depends(Authorisation())
    dummy_auth = {"sub": "eval_user", "roles": ["evaluator"]}

    for question in questions:
        # 1) Fetch context for each question
        user_context = await fetch_context(question)

        # 2) Build the request including required context
        req = UserQuestionRequest(
            user_question=question,
            user_context=user_context
        )

        # 3) Call the live pipeline handler
        resp = await handle_query(req, auth=dummy_auth)

        # 4) Convert response to dict and include the question
        record = resp.dict() if hasattr(resp, "dict") else dict(resp)
        record["Question"] = question
        results.append(record)

    # Return as DataFrame for writing or printing
    return pd.DataFrame(results)


def main():
    # Use hard-coded test questions rather than reading from a file
    questions = get_test_questions()

    # Run the async evaluation batch
    df_out = asyncio.run(_evaluate_batch(questions))

    # Write results out to console and to a CSV for now
    print("===== Evaluation Results =====")
    print(df_out.to_string(index=False))

    # Save to CSV in the working directory
    output_file = os.getenv("EVAL_OUTPUT_FILE", "eval_results.csv")
    df_out.to_csv(output_file, index=False)
    print(f"✅ Wrote {len(df_out)} evaluation records to {output_file}")


if __name__ == "__main__":
    main()
