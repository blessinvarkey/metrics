```python
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
    QA_PROMPT_TEMPLATE,
    QA_PROMPT_RAILS_MAP,
    OpenAIModel,
    LiteLLMModel,
    llm_classify
)

# Configuration
SCRIPT_DIR = os.path.dirname(__file__)
DATASET_PATH = os.getenv(
    "EVAL_DATASET_FILE",
    os.path.join(SCRIPT_DIR, "eval_dataset.xlsx")
)
USE_HARDCODE = os.getenv("USE_HARDCODE_EXAMPLES", "false").lower() == "true"

# Hard-coded examples (for quick testing)
HARD_CODED_DATA = [
    {
        "Question": "How many users signed up last month?",
        "GroundTruthSQL": "SELECT COUNT(*) FROM users WHERE signup_date BETWEEN '2025-05-01' AND '2025-05-31';",
        "GroundTruthResponse": [{"count": 1234}]
    },
]

# Load evaluation data
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

# 1) Generate predictions via your pipeline
async def generate_predictions(dataf: pd.DataFrame) -> pd.DataFrame:
    dummy_auth = {"sub": "eval_user", "roles": ["evaluator"]}
    recs = []
    for _, row in dataf.iterrows():
        q = row["Question"]
        ctx = await fetch_context(q)
        req = UserQuestionRequest(user_question=q, user_context=str(ctx))
        resp = await handle_query(req, auth=dummy_auth)
        out = resp.dict() if hasattr(resp, "dict") else dict(resp)
        recs.append({
            "Question": q,
            "GroundTruthSQL": row.get("GroundTruthSQL"),
            "GroundTruthResponse": row.get("GroundTruthResponse"),
            "GeneratedSQL": out.get("final_sql") or out.get("sql_query"),
            "GeneratedResponse": out.get("query_execution_response") or out.get("rows")
        })
    return pd.DataFrame(recs)

pred_df = asyncio.run(generate_predictions(df))

# 2) Evaluate SQL correctness vs GroundTruthSQL with llm_classify
# Prepare classification DataFrame
classify_sql_df = pred_df[["Question", "GeneratedSQL", "GroundTruthSQL"]].rename(
    columns={"Question": "instruction", "GeneratedSQL": "predicted_sql", "GroundTruthSQL": "ground_truth_sql"}
)

# Instantiate models
eval_azure = OpenAIModel(
    model_name=os.getenv("AZURE_OPENAI_CHAT_MODEL"),
    temperature=float(os.getenv("TEMPERATURE", 0)),
    api_base=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_KEY"),
    api_type="azure",
    api_version=os.getenv("AZURE_OPENAI_VERSION")
)
eval_ollama = LiteLLMModel(model=os.getenv("OLLAMA_MODEL", "ollama/llama3.2-vision:11b"))

rails = list(SQL_GEN_EVAL_PROMPT_RAILS_MAP.values())
# Azure evaluation
results_azure = llm_classify(
    dataframe=classify_sql_df,
    template=SQL_GEN_EVAL_PROMPT_TEMPLATE,
    model=eval_azure,
    rails=rails,
    provide_explanation=True,
)
# Ollama evaluation
results_ollama = llm_classify(
    dataframe=classify_sql_df,
    template=SQL_GEN_EVAL_PROMPT_TEMPLATE,
    model=eval_ollama,
    rails=rails,
    provide_explanation=True,
)

# Merge labels & explanations
eval_df = pred_df.copy()
eval_df['label_azure'] = results_azure['label']
eval_df['explanation_azure'] = results_azure['explanation']
eval_df['label_ollama'] = results_ollama['label']
eval_df['explanation_ollama'] = results_ollama['explanation']

# 3) Output
# Terminal
print("\n===== Detailed Evaluation Results =====")
print(eval_df.to_string(index=False))

# Aggregate metrics
metrics_azure = results_azure.metrics()
metrics_ollama = results_ollama.metrics()
print("\n===== Aggregate Metrics =====")
print("-- Azure OpenAI --")
for k, v in metrics_azure.items(): print(f"{k:30s}: {v}")
print("\n-- Ollama --")
for k, v in metrics_ollama.items(): print(f"{k:30s}: {v}")

# Excel export
out_path = os.path.join(SCRIPT_DIR, "eval_results.xlsx")
with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
    eval_df.to_excel(writer, sheet_name="Detailed", index=False)
    # Summary sheet
    summary = [
        *[(f"Azure: {k}", v) for k, v in metrics_azure.items()],
        ("", ""),
        *[(f"Ollama: {k}", v) for k, v in metrics_ollama.items()]
    ]
    pd.DataFrame(summary, columns=["Metric", "Value"]).to_excel(
        writer, sheet_name="Summary", index=False
    )

print(f"\nExcel report saved to: {out_path}\n")
```
