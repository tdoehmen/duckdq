import duckdb

from duckdq.engines.state_engine import StateEngine
from duckdq.utils.analysis_runner import AnalyzerContext
from duckdq.utils.exceptions import StateMergingException
from duckdq.verification_suite import VerificationResult, VerificationSuite

con = duckdb.connect("basic_check.duckdq")
state_engine = StateEngine(con)
val_runs = state_engine.get_validation_runs()
run_ids = []
check = val_runs[0]["check"]
required_properties = val_runs[0]["required_properties"]
for val_run in val_runs:
    run_id = val_run["run_id"]
    run_ids.append(run_id)
    if not val_run["required_properties"].issubset(required_properties):
        raise StateMergingException(f"Merging of States for run_id {run_id} not possible. Check constains properties not contained in other runs.")

property_state_map = state_engine.merge_states(run_ids,required_properties)
print(property_state_map)
metrics_by_property = state_engine.metrics_from_states(property_state_map)
analyzer_context = AnalyzerContext(metrics_by_property)
verification_result: VerificationResult = VerificationSuite().evaluate([check],analyzer_context)
print(verification_result)

