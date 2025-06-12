# backend/app/scripts/eval/eval.py

import os
import asyncio
import pandas as pd
import json
from services.pipeline_service import generate_refine_execute

# File paths (override via env)
INPUT_PATH  = os.getenv("QUESTIONS_FILE", "data/questions.xlsx")
OUTPUT_PATH = os.getenv("EVAL_OUTPUT_FILE", "data/eval_results.xlsx")
SHEET_NAME  = os.getenv("QUESTIONS_SHEET", "Sheet1")

async def _evaluate_questions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run the full generate-refine-execute pipeline for each question
    and collect results in a DataFrame.
    """
    records = []
    for _, row in df.iterrows():
        question = row["Question"]
        try:
            # Call the end-to-end pipeline
            result = await generate_refine_execute(question)
        except Exception as e:
            # In case of unexpected errors
            result = {
                "initial_sql": None,
                "refined_sql": None,
                "final_sql": None,
                "pipeline_status": "error",
                "error": str(e),
                "rows": None
            }
        # Build a flat record
        rec = {
            "Question": question,
            "InitialSQL": result.get("initial_sql"),
            "RefinedSQL": result.get("refined_sql"),
            "FinalSQL": result.get("final_sql"),
            "Status": result.get("pipeline_status"),
            "Error": result.get("error"),
            "RowsReturned": len(result.get("rows") or [])
        }
        records.append(rec)

    return pd.DataFrame(records)


def main():
    # 1) Load questions from Excel or CSV
    if INPUT_PATH.lower().endswith(".xlsx"):
        df = pd.read_excel(INPUT_PATH, sheet_name=SHEET_NAME)
    else:
        df = pd.read_csv(INPUT_PATH)

    if "Question" not in df.columns:
        raise ValueError("Input file must have a 'Question' column")

    # 2) Run the async evaluation
    df_results = asyncio.run(_evaluate_questions(df))

    # 3) Write out the results
    if OUTPUT_PATH.lower().endswith(".xlsx"):
        with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
            df_results.to_excel(writer, sheet_name="EvalResults", index=False)
    else:
        df_results.to_csv(OUTPUT_PATH, index=False)

    print(f"✅ Evaluation complete: {len(df_results)} questions processed")
    print(f"▶️  Results written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
