from sklearn.svm import SVC
from sklearn.preprocessing import StandardScaler
from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
from duckdq.sklearn import DQPipeline, Assertion, Type


df = pd.read_csv("data/train.csv")
y = df["Survived"]
X = df.drop("Survived",axis=1)
X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=0)

features_numericas = ['Age', 'Fare', 'SibSp', 'Parch']
features_categoricas = ['Embarked', 'Sex', 'Pclass']
features_para_remover = ['Name', 'Cabin', 'Ticket', 'PassengerId']

numeric_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='median')),
    ('scaler', MinMaxScaler())])

categorical_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='most_frequent')),
    ('onehot', OneHotEncoder())])

preprocessor = ColumnTransformer(
    transformers=[
        ('Features numericas', numeric_transformer, features_numericas),
        ('Features categoricas', categorical_transformer, features_categoricas),
        ('Feature para remover', 'drop', features_para_remover)
    ])


inp_assert = (Assertion(Type.EXCEPTION,description="Basic Check")
              .is_complete("Name")
              .is_contained_in("Pclass",(1,2,3))
              .is_contained_in("Sex",("male","female"))
              .is_contained_in("SibSp",[1, 0, 3, 4, 2, 5, 8])
              .is_contained_in("Embarked",("S","C","Q"))
              .has_min("Age", lambda mn: mn > 0)
              .has_max("Age", lambda mx: mx < 0)
              .has_min("Fare", lambda mn: mn >= 0)
              .has_max("Fare", lambda mx: mx < 999)
              .is_unique("PassengerId")
              .is_unique("Name"))

outp_assert = (Assertion(Type.WARNING,description="Basic Check")
               # check ratio of positives is less than 10%
               .is_unique("y"))


from sklearn.linear_model import LogisticRegression

pipe = DQPipeline([('preprocessor', preprocessor),
                   ('clf', LogisticRegression(solver='liblinear')),
                   ],
                  input_assertion=inp_assert,
                  output_assertion=outp_assert)

pipe.fit(X_train, y_train)