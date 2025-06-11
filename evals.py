# backend/app/scripts/generate_outputs.py

import os
import asyncio
import pandas as pd

from services.sql_generation import get_completion_from_messages

# File paths (override via env if you like)
INPUT_PATH  = os.getenv("QUESTIONS_FILE", "data/questions.xlsx")
OUTPUT_PATH = os.getenv("MODEL_OUTPUT_FILE", "data/questions_with_responses.xlsx")
SHEET_NAME  = os.getenv("QUESTIONS_SHEET", "Sheet1")

# You can supply any system prompt your pipeline expects,
# for example a Jinja-rendered context from your prompt_service.
SYSTEM_PROMPT = os.getenv(
    "LLM_SYSTEM_PROMPT",
    "You are a SQL generator. Given the user question, output a valid SQL query."
)

async def _generate_responses(df: pd.DataFrame) -> pd.DataFrame:
    """
    Iterate over each row in df (which must have a 'Question' column),
    call get_completion_from_messages, and append 'ModelResponse'.
    """
    responses = []
    for idx, row in df.iterrows():
        user_q = row["Question"]
        try:
            resp = await get_completion_from_messages(
                system_message=SYSTEM_PROMPT,
                user_message=user_q
            )
        except Exception as e:
            resp = f"<ERROR: {e}>"
        responses.append(resp)
    df["ModelResponse"] = responses
    return df

def main():
    # 1) Load questions
    if INPUT_PATH.lower().endswith(".xlsx"):
        df = pd.read_excel(INPUT_PATH, sheet_name=SHEET_NAME)
    else:
        df = pd.read_csv(INPUT_PATH)

    if "Question" not in df.columns:
        raise ValueError("Input file must have a 'Question' column")

    # 2) Run the async generation
    df_out = asyncio.run(_generate_responses(df))

    # 3) Write results back out
    if OUTPUT_PATH.lower().endswith(".xlsx"):
        with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
            df_out.to_excel(writer, sheet_name="WithResponses", index=False)
    else:
        df_out.to_csv(OUTPUT_PATH, index=False)

    print(f"✅ Generated responses for {len(df_out)} questions")
    print(f"▶️  Results written to {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
