# services/pipeline_service.py

from services.sql_generation import get_completion_from_messages
from services.query_execution import execute_sql
from services.refiner_service import refine_sql

async def generate_refine_execute(question: str) -> dict:
    # 1) First‐pass SQL
    initial_sql = await get_completion_from_messages(system_message, question)
    # 2) Try to run it
    try:
        result = await execute_sql(initial_sql)
        return {
          "initial_sql": initial_sql,
          "refined_sql": None,
          "final_sql":   initial_sql,
          "pipeline_status": "success",
          "error": None,
          "rows": result,
        }
    except Exception as err:
        # 3) On error, call the refiner
        refined_sql = await refine_sql(initial_sql, question)
        # 4) Try again
        try:
            result = await execute_sql(refined_sql)
            return {
              "initial_sql": initial_sql,
              "refined_sql": refined_sql,
              "final_sql":   refined_sql,
              "pipeline_status": "refined_success",
              "error": None,
              "rows": result,
            }
        except Exception as err2:
            return {
              "initial_sql": initial_sql,
              "refined_sql": refined_sql,
              "final_sql": None,
              "pipeline_status": "failed",
              "error": str(err2),
              "rows": None,
            }
