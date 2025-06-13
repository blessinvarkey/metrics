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

# === Hard-coded evaluation cases ===
# Toggle via USE_HARDCODE_EXAMPLES=true
USE_HARDCODE = os.getenv("USE_HARDCODE_EXAMPLES", "false").lower() == "true"

TEST_EVAL_CASES = [
    {
        "Question": "How many users signed up last month?",
        "UserContext": None,
        "GroundTruthSQL": (
            "SELECT COUNT(*) FROM users "
            "WHERE signup_date BETWEEN '2025-05-01' AND '2025-05-31';"
        ),
        "GroundTruthResponse": [{"count": 1234}],
    },
    {
        "Question": "How many lapsed policies came from penn?",
        "UserContext": None,
        "GroundTruthSQL": (
            "SELECT COUNT(*) FROM policies "
            "WHERE status = 'lapsed' AND source = 'penn';"
        ),
        "GroundTruthResponse": [{"count": 567}],
    },
]

if USE_HARDCODE:
    eval_df = pd.DataFrame(TEST_EVAL_CASES)
else:
    raise RuntimeError("USE_HARDCODE_EXAMPLES must be true to use hard-coded cases")

# === Step 1: Run through Conversational BI pipeline ===
async def run_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    dummy_auth = {"sub": "eval_user", "roles": ["evaluator"]}
    records = []
    for _, row in df.iterrows():
        q = row["Question"]
        # fetch optional context
        ctx = await fetch_context(q)
        user_ctx = str(ctx) if ctx is not None else None
        # build request
        req = UserQuestionRequest(user_question=q, user_context=user_ctx)
        # execute full pipeline
        resp = await handle_query(req, auth=dummy_auth)
        out = resp.dict() if hasattr(resp, "dict") else dict(resp)
        records.append({
            "Question": q,
            "UserContext": user_ctx,
            "GeneratedSQL": out.get("final_sql") or out.get("sql_query"),
            "GeneratedResponse": out.get("query_execution_response") or out.get("rows"),
            # include ground-truth for later eval
            "GroundTruthSQL": row.get("GroundTruthSQL"),
            "GroundTruthResponse": row.get("GroundTruthResponse"),
        })
    return pd.DataFrame(records)

# execute pipeline
pred_df = asyncio.run(run_pipeline(eval_df))

# === Step 2: Evaluate with Arize Phoenix ===
# Prepare DataFrames for SQL correctness eval
sql_df = pred_df[["Question", "GeneratedSQL", "GroundTruthSQL"]].rename(
    columns={"Question": "instruction", "GeneratedSQL": "predicted_sql", "GroundTruthSQL": "ground_truth_sql"}
)
# Prepare DataFrames for response correctness eval
resp_df = pred_df[["Question", "GeneratedResponse", "GroundTruthResponse"]].rename(
    columns={"Question": "instruction", "GeneratedResponse": "predicted_response", "GroundTruthResponse": "ground_truth_response"}
)

# Instantiate Azure and Ollama models
azure_model = OpenAIModel(
    model_name=os.getenv("AZURE_OPENAI_CHAT_MODEL"),
    temperature=float(os.getenv("TEMPERATURE", "0")),
    openai_api_type="azure",
    openai_api_base=os.getenv("AZURE_OPENAI_ENDPOINT"),
    openai_api_key=os.getenv("AZURE_OPENAI_KEY"),
    openai_api_version=os.getenv("AZURE_OPENAI_VERSION"),
),
    temperature=float(os.getenv("TEMPERATURE", "0")),
    api_base=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_KEY"),
    api_type="azure",
    api_version=os.getenv("AZURE_OPENAI_VERSION"),
)
ollama_model = LiteLLMModel(model=os.getenv("OLLAMA_MODEL", "ollama/llama3.2-vision:11b"))
rails_sql = list(SQL_GEN_EVAL_PROMPT_RAILS_MAP.values())
rails_qa = list(QA_PROMPT_RAILS_MAP.values())

# SQL evaluation
sql_res_azure = llm_classify(
    dataframe=sql_df,
    template=SQL_GEN_EVAL_PROMPT_TEMPLATE,
    model=azure_model,
    rails=rails_sql,
    provide_explanation=True,
)
sql_res_ollama = llm_classify(
    dataframe=sql_df,
    template=SQL_GEN_EVAL_PROMPT_TEMPLATE,
    model=ollama_model,
    rails=rails_sql,
    provide_explanation=True,
)
# Response evaluation
resp_res_azure = llm_classify(
    dataframe=resp_df,
    template=QA_PROMPT_TEMPLATE,
    model=azure_model,
    rails=rails_qa,
    provide_explanation=True,
)
resp_res_ollama = llm_classify(
    dataframe=resp_df,
    template=QA_PROMPT_TEMPLATE,
    model=ollama_model,
    rails=rails_qa,
    provide_explanation=True,
)

# Merge labels back into pred_df
result_df = pred_df.copy()
result_df["label_sql_azure"] = sql_res_azure["label"]
result_df["explanation_sql_azure"] = sql_res_azure["explanation"]
result_df["label_sql_ollama"] = sql_res_ollama["label"]
result_df["explanation_sql_ollama"] = sql_res_ollama["explanation"]
result_df["label_resp_azure"] = resp_res_azure["label"]
result_df["explanation_resp_azure"] = resp_res_azure["explanation"]
result_df["label_resp_ollama"] = resp_res_ollama["label"]
result_df["explanation_resp_ollama"] = resp_res_ollama["explanation"]

# === Step 3: Output ===
# Terminal output
print("\n===== Detailed Evaluation Results =====")
print(result_df.to_string(index=False))

# Aggregate metrics
metrics = {}
metrics['Azure SQL'] = sql_res_azure.metrics()
metrics['Ollama SQL'] = sql_res_ollama.metrics()
metrics['Azure Resp'] = resp_res_azure.metrics()
metrics['Ollama Resp'] = resp_res_ollama.metrics()
print("\n===== Aggregate Metrics =====")
for key, met in metrics.items():
    print(f"-- {key} --")
    for k, v in met.items(): print(f"{k:30s}: {v}")
    print()

# Excel export
out_path = os.path.join(os.path.dirname(__file__), "eval_results.xlsx")
with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
    result_df.to_excel(writer, sheet_name="Detailed", index=False)
    # summary
    sum_rows = []
    for key, met in metrics.items():
        for k, v in met.items():
            sum_rows.append((f"{key}: {k}", v))
    pd.DataFrame(sum_rows, columns=["Metric","Value"]).to_excel(
        writer, sheet_name="Summary", index=False
    )

print(f"\nExcel report saved to: {out_path}\n")
```
