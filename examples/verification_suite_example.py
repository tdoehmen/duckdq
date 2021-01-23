import time

import pandas as pd
from duckdq.checks import Check, CheckLevel
from duckdq.verification_suite import VerificationSuite

start = time.time()
df = pd.read_csv("data/train.csv")
end = time.time()
print(end-start)

start = time.time()
verification_result = (
    VerificationSuite()
        .on_data(df, dataset_id="data10", partition_id="1")
        .using_metadata_repository("duckdb://basic_check.duckdq")
        .add_check(
        Check(CheckLevel.EXCEPTION, "Basic Check")
            .is_complete("Name")
            .is_contained_in("Pclass",(1,2,3))
            .is_contained_in("Sex",("male","female"))
            .is_contained_in("SibSp",[1, 0, 3, 4, 2, 5, 8])
            .is_contained_in("Embarked",("S","C","Q"))
            .has_min("Age", lambda mn: mn > 0)
            .has_max("Age", lambda mx: mx < 60)
            .has_min("Fare", lambda mn: mn >= 0)
            .has_max("Fare", lambda mx: mx < 999)
    )
    .run()
)
end = time.time()
print(end-start)
