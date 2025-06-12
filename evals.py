# backend/app/services/evals.py

import os
import asyncio
import pandas as pd

from api.queries import main as handle_query
from api.models import UserQuestionRequest

# Paths & sheet names (override via env if desired)
INPUT_PATH  = os.getenv("QUESTIONS_FILE",  "data/questions.xlsx")
OUTPUT_PATH = os.getenv("EVAL_OUTPUT_FILE", "data/eval_results.xlsx")
SHEET_NAME  = os.getenv("QUESTIONS_SHEET",  "Sheet1")

async def _evaluate_batch(df: pd.DataFrame):
    """
    Call the full production pipeline (api.queries.main) for each question.
    """
    results = []

    # A minimal auth dict that passes your Authorization() dependency
    dummy_auth = {"sub": "eval_user", "roles": ["evaluator"]}

    for question in df["Question"]:
        # Wrap the question in the same request object your endpoint expects
        req = UserQuestionRequest(user_question=question)
        # Call your live pipeline handler
        resp = await handle_query(req, auth=dummy_auth)
        # resp is likely a Pydantic model or dict containing:
        # { initial_sql, refined_sql, final_rows, confidence_score, status, timestamps, ... }
        record = resp.dict() if hasattr(resp, "dict") else dict(resp)
        record["Question"] = question
        results.append(record)

    return pd.DataFrame(results)


def main():
    # 1) Load questions from Excel or CSV
    if INPUT_PATH.lower().endswith(".xlsx"):
        df = pd.read_excel(INPUT_PATH, sheet_name=SHEET_NAME)
    else:
        df = pd.read_csv(INPUT_PATH)

    if "Question" not in df.columns:
        raise ValueError("Input file must have a 'Question' column")

    # 2) Run the async evaluation batch
    df_out = asyncio.run(_evaluate_batch(df))

    # 3) Write full results out
    if OUTPUT_PATH.lower().endswith(".xlsx"):
        with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
            df_out.to_excel(writer, index=False, sheet_name="EvalResults")
    else:
        df_out.to_csv(OUTPUT_PATH, index=False)

    print(f"✅ Wrote {len(df_out)} evaluation records to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
