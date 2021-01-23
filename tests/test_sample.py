# Apache Software License 2.0
#
# Modifications copyright (C) 2021, Till Döhmen, Fraunhofer FIT
# Copyright (c) 2019, Miguel Cabrera
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pandas as pd

from duckdq.checks import Check, CheckLevel, CheckStatus
from duckdq.constraints import ConstraintStatus
from duckdq.verification_suite import VerificationSuite

df = pd.DataFrame(
    [
        (1, "Thingy A", "awesome thing.", "high", 0),
        (2, "Thingy B", "available at http://thingb.com", None, 0),
        (3, None, None, "low", 5),
        (4, "Thingy D", "checkout https://thingd.ca", "low", 10),
        (5, "Thingy E", None, "high", 12),
    ],
    columns=["id", "productName", "description", "priority", "numViews"],
)


def test_sample():

    verification_result = (
        VerificationSuite()
        .on_data(df)
        .add_check(
            Check(CheckLevel.EXCEPTION, "Basic Check")
            .has_size(lambda sz: sz == 5)  # we expect 5 rows
            .is_complete("id")  # should never be None/Null
            .is_unique("id")  # should not contain duplicates (NOT YET IMPLEMENTED)
            .is_complete("productName")  # should never be None/Null
            .is_contained_in("priority", ("high", "low"))
            .is_non_negative("numViews")
            .has_max("id", lambda mx: mx == 5)
            .has_min("id", lambda mn: mn == 1)
            .has_min_length("description", lambda mlen: mlen > 3)
            .has_max_length("description", lambda mxlen: mxlen < 30)
            # .contains_url("description", lambda d: d >= 0.5) (NOT YET IMPLEMENTED)
            #.has_quantile("numViews", 0.5, lambda v: v <= 10)
        )
        .run()
    )

    assert verification_result.status == CheckStatus.ERROR

    print()
    for check_result in verification_result.check_results.values():
        for cr in check_result.constraint_results:
            if cr.status != ConstraintStatus.SUCCESS:
                print(f"{cr.constraint}: {cr.message}")
