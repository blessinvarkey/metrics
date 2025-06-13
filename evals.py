# backend/app/services/evals.py

import os
import asyncio
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

from api.queries import main as handle_query, fetch_context
from models.entities import UserQuestionRequest
from phoenix.evals import (
    SQL_GEN_EVAL_PROMPT_TEMPLATE,
    SQL_GEN_EVAL_PROMPT_RAILS_MAP,
    OpenAIModel,
    LiteLLMModel,
    llm_classify
)
# Inline DataSchema definition (avoids missing phoenix.utils)
class DataSchema:
    def __init__(self, prediction_id_column_name: str, prediction_label_column_name: str, actual_label_column_name: str):
        self.prediction_id_column_name = prediction_id_column_name
        self.prediction_label_column_name = prediction_label_column_name
        self.actual_label_column_name = actual_label_column_name
from phoenix.evals import PhoenixEvalConfig, MultipleChoiceEval
import utilities.constants as constants

# === Configuration ===
SCRIPT_DIR = os.path.dirname(__file__)
# Path to dataset with ground truth
DATASET_PATH = os.getenv(
    "EVAL_DATASET_FILE",
    os.path.join(SCRIPT_DIR, "eval_dataset.xlsx")
)
# Flag to use hard-coded examples instead of loading file
USE_HARDCODE = os.getenv("USE_HARDCODE_EXAMPLES", "false").lower() == "true"

# === Hard-coded examples (for quick testing) ===
HARD_CODED_DATA = [
    {
        "Question": "How many users signed up last month?",
        "GroundTruthSQL": "SELECT COUNT(*) FROM users WHERE signup_date BETWEEN '2025-05-01' AND '2025-05-31';",
        "GroundTruthResponse": [{"count": 1234}]
    },
    # Add more examples as needed
]

# === Load evaluation data ===
if USE_HARDCODE:
    df = pd.DataFrame(HARD_CODED_DATA)
else:
    if DATASET_PATH.lower().endswith(".xlsx"):
        df = pd.read_excel(DATASET_PATH)
    else:
        df = pd.read_csv(DATASET_PATH)

    required_cols = {"Question", "GroundTruthSQL", "GroundTruthResponse"}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"Dataset must contain columns: {required_cols}")

# === 1) Run pipeline to generate predictions ===
async def generate_predictions(dataf: pd.DataFrame) -> pd.DataFrame:
    dummy_auth = {"sub": "eval_user", "roles": ["evaluator"]}
    recs = []
    for _, row in dataf.iterrows():
        q = row["Question"]
        # fetch context
        ctx = await fetch_context(q)
        user_context = str(ctx)
        # build request and call pipeline
        req = UserQuestionRequest(user_question=q, user_context=user_context)
        resp = await handle_query(req, auth=dummy_auth)
        d = resp.dict() if hasattr(resp, "dict") else dict(resp)
        generated_sql = d.get("final_sql") or d.get("sql_query")
        generated_response = d.get("query_execution_response") or d.get("rows")
        rec = {
            "Question": q,
            "GroundTruthSQL": row.get("GroundTruthSQL"),
            "GroundTruthResponse": row.get("GroundTruthResponse"),
            "GeneratedSQL": generated_sql,
            "GeneratedResponse": generated_response
        }
        recs.append(rec)
    return pd.DataFrame(recs)

pred_df = asyncio.run(generate_predictions(df))

# === 2) Evaluate SQL correctness vs GroundTruthSQL ===
# Prepare DataFrame for classification
classify_df = pred_df[["Question", "GeneratedSQL", "GroundTruthSQL"]].rename(
    columns={
        "Question": "prediction_id",
        "GeneratedSQL": "predicted_sql",
        "GroundTruthSQL": "ground_truth_sql"
    }
)

# Build Phoenix eval models
azure_model = OpenAIModel(
    model_name=os.getenv("AZURE_OPENAI_CHAT_MODEL"),
    temperature=float(os.getenv("TEMPERATURE", 0)),
    api_base=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_KEY"),
    api_type="azure",
    api_version=os.getenv("AZURE_OPENAI_VERSION")
)
ollama_model = LiteLLMModel(model=os.getenv("OLLAMA_MODEL", "ollama/llama3.2-vision:11b"))

schema = DataSchema(
    prediction_id_column_name="prediction_id",
    prediction_label_column_name="predicted_sql",
    actual_label_column_name="ground_truth_sql"
)
eval_config = PhoenixEvalConfig()
evaluator_azure = MultipleChoiceEval(
    llm_model=azure_model,
    data_schema=schema,
    config=eval_config
)
evaluator_ollama = MultipleChoiceEval(
    llm_model=ollama_model,
    data_schema=schema,
    config=eval_config
)

# Run evaluations
results_azure = evaluator_azure.run(classify_df)
results_ollama = evaluator_ollama.run(classify_df)
metrics_azure = results_azure.metrics()
metrics_ollama = results_ollama.metrics()
metrics_df_azure = results_azure.metrics_df
metrics_df_ollama = results_ollama.metrics_df

# Merge back labels & explanations
eval_df = pred_df.copy()
eval_df["label_azure"] = metrics_df_azure["label"]
eval_df["explanation_azure"] = metrics_df_azure["explanation"]
eval_df["label_ollama"] = metrics_df_ollama["label"]
eval_df["explanation_ollama"] = metrics_df_ollama["explanation"]

# === 3) Output ===
# 3a) Terminal
print("\n===== Detailed Evaluation Results =====")
print(eval_df.to_string(index=False))

print("\n===== Aggregate Metrics =====")
print("-- Azure OpenAI --")
for k, v in metrics_azure.items(): print(f"{k:30s}: {v}")
print("\n-- Ollama --")
for k, v in metrics_ollama.items(): print(f"{k:30s}: {v}")

# 3b) Excel export
out_path = os.path.join(SCRIPT_DIR, "eval_results.xlsx")
with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
    eval_df.to_excel(writer, sheet_name="Detailed", index=False)
    summary = [
        ("Metric (Azure)", "Value"),
        *metrics_azure.items(),
        ("", ""),
        ("Metric (Ollama)", "Value"),
        *metrics_ollama.items()
    ]
    pd.DataFrame(summary, columns=["Metric", "Value"]).to_excel(
        writer, sheet_name="Summary", index=False
    )

print(f"\nExcel report saved to: {out_path}\n")

if __name__ == "__main__":
    pass
```
