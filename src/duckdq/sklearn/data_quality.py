import warnings
from dataclasses import dataclass
from enum import Enum

import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline
from duckdq import VerificationSuite
from duckdq.checks import CheckStatus, Check, CheckLevel
from duckdq.utils.exceptions import DataQualityException


class DataQuality(BaseEstimator, TransformerMixin):

    def __init__(self, check: Check):
        self.check = check

    # the arguments are ignored anyway, so we make them optional
    def fit(self, X=None, y=None):
        #TODO: validate x+y here, then drop y-related constraint
        return self

    def transform(self, X):
        db_name = "default"
        if self.check.description is not None and self.check.description != '':
            db_name = self.check.description.replace(" ","_").lower()
        dataset_id = None
        if hasattr(X, 'dataset_id'):
            dataset_id = X.dataset_id
        verification_result = VerificationSuite().on_data(X,dataset_id=dataset_id).using_metadata_repository(f"duckdb://{db_name}.duckdq").add_check(self.check).run()
        if verification_result.status == CheckStatus.ERROR:
            raise DataQualityException(f"Error: Data quality validation failed and pipeline was stopped. Details: \n{str(verification_result)}")
        elif verification_result.status == CheckStatus.WARNING:
            warnings.warn(f"Warning: Data quality validation failed. Details: \n{str(verification_result)}")
        return X


class Assertion(Check):
    pass

class DQPipeline(Pipeline):

    def __init__(self, steps, *, memory=None, verbose=False, input_assertion: Check=None, output_assertion: Check=None):
        self.input_check = input_assertion
        self.output_check = output_assertion
        super().__init__(steps, memory=memory, verbose=verbose)

    def check_input(self, X, y=None):
        db_name = "default"
        if self.input_check.description is not None and self.input_check.description != '':
            db_name = self.input_check.description.replace(" ","_").lower()
        verification_result = VerificationSuite() \
            .on_data(X) \
            .using_metadata_repository(f"duckdb://{db_name}.duckdq") \
            .add_check(self.input_check).run()
        if verification_result.status == CheckStatus.ERROR:
            raise DataQualityException(f"Error: Data quality validation failed and pipeline was stopped. Details: \n{str(verification_result)}")
        elif verification_result.status == CheckStatus.WARNING:
            warnings.warn(f"Warning: Data quality validation failed. Details: \n{str(verification_result)}")

    def check_output(self, y):
        db_name = "default"
        if self.output_check.description is not None and self.output_check.description != '':
            db_name = self.output_check.description.replace(" ","_").lower()
        verification_result = VerificationSuite() \
            .on_data(y) \
            .using_metadata_repository(f"duckdb://{db_name}.duckdq") \
            .add_check(self.output_check).run()
        if verification_result.status == CheckStatus.ERROR:
            raise DataQualityException(f"Error: Data quality validation failed and pipeline was stopped. Details: \n{str(verification_result)}")
        elif verification_result.status == CheckStatus.WARNING:
            warnings.warn(f"Warning: Data quality validation failed. Details: \n{str(verification_result)}")

    def fit(self, X, y=None, **fit_params):
        self.check_input(X,y)
        return super().fit(X, y=y, **fit_params)

    def predict(self, X, **predict_params):
        self.check_input(X)
        y = super().predict(X,**predict_params)
        y_df = pd.DataFrame(y,columns=["y"])
        self.check_output(y_df)
        return y


class AssertionPipelineStep(BaseEstimator, TransformerMixin):

    def __init__(self, check: Check):
        self.check = check

    # the arguments are ignored anyway, so we make them optional
    def fit(self, X=None, y=None):
        #TODO: validate x+y here, then drop y-related constraint
        return self

    def transform(self, X):
        db_name = "default"
        if self.check.description is not None and self.check.description != '':
            db_name = self.check.description.replace(" ","_").lower()
        dataset_id = None
        if hasattr(X, 'dataset_id'):
            dataset_id = X.dataset_id
        verification_result = VerificationSuite().on_data(X,dataset_id=dataset_id).using_metadata_repository(f"duckdb://{db_name}.duckdq").add_check(self.check).run()
        if verification_result.status == CheckStatus.ERROR:
            raise DataQualityException(f"Error: Data quality validation failed and pipeline was stopped. Details: \n{str(verification_result)}")
        elif verification_result.status == CheckStatus.WARNING:
            warnings.warn(f"Warning: Data quality validation failed. Details: \n{str(verification_result)}")
        return X
