# backend/app/services/evals.py

import os
import asyncio
import pandas as pd

# Allow locating the questions file relative to this script\ nSCRIPT_DIR = os.path.dirname(__file__)

# Import the same entry-point your API uses
from api.queries import main as handle_query, fetch_context
from models.entities import UserQuestionRequest

# File paths (override via env)
INPUT_PATH      = os.getenv(
    "QUESTIONS_FILE",
    os.path.join(SCRIPT_DIR, "questions.xlsx")
)
OUTPUT_PATH     = os.getenv(
    "EVAL_OUTPUT_FILE",
    os.path.join(SCRIPT_DIR, "eval_results.xlsx")
)
SHEET_NAME      = os.getenv("QUESTIONS_SHEET", None)
QUESTION_COLUMN = os.getenv("QUESTION_COLUMN", None)

async def _evaluate_batch(df: pd.DataFrame):
    """
    Call the full production pipeline (api.queries.main) for each question.
    """
    results = []

    # Minimal auth dict to satisfy Depends(Authorisation())
    dummy_auth = {"sub": "eval_user", "roles": ["evaluator"]}

    for question in df["Question"]:
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

    return pd.DataFrame(results)


def main():
    # 1) Load questions from Excel or CSV
    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(f"Questions file not found: {INPUT_PATH}")

    # Read file
    if INPUT_PATH.lower().endswith(".xlsx"):
        df = pd.read_excel(INPUT_PATH, sheet_name=SHEET_NAME)
    else:
        df = pd.read_csv(INPUT_PATH)

    # 2) Determine which column holds questions
    cols = list(df.columns)
    if QUESTION_COLUMN and QUESTION_COLUMN in cols:
        qcol = QUESTION_COLUMN
    elif "Question" in cols:
        qcol = "Question"
    elif "question" in cols:
        qcol = "question"
    elif len(cols) == 1:
        qcol = cols[0]
    else:
        raise ValueError(
            "Could not find question column. Set QUESTION_COLUMN or include 'Question' or 'question'. "
            f"Available columns: {cols}"
        )

    # Normalize to 'Question'
    if qcol != "Question":
        df = df.rename(columns={qcol: "Question"})

    # 3) Run the async evaluation batch
    df_out = asyncio.run(_evaluate_batch(df))

    # 4) Write results out to Excel or CSV
    if OUTPUT_PATH.lower().endswith(".xlsx"):
        with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
            df_out.to_excel(writer, index=False, sheet_name="EvalResults")
    else:
        df_out.to_csv(OUTPUT_PATH, index=False)

    print(f"✅ Wrote {len(df_out)} evaluation records to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
