import warnings
from sklearn.base import BaseEstimator, TransformerMixin
from duckdq import VerificationSuite
from duckdq.checks import CheckStatus, Check
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


