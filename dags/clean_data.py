from sqlalchemy import create_engine
import pandas as pd
import boto3, logging, botocore
from botocore.config import Config
import numpy as np


def get_s3_client():
    s3 = boto3.client('s3',
                      endpoint_url='http://minio:9000',
                      aws_access_key_id='dataopsadmin',
                      aws_secret_access_key='dataopsadmin',
                      config=Config(signature_version='s3v4'))
    return s3

def load_df_from_s3(bucket, key, s3,sep=",",index_col=None, usecols=None):
    ''' Read a csv from s3 bucket & load into pandas dataframe''' 
    try:
        logging.info(f"Loading {bucket, key}")
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(obj['Body'], sep=sep, index_col=index_col, usecols=usecols, low_memory=False)
    except botocore.exceptions.ClientError as err:
        status = err.response["ResponseMetadata"]["HTTPStatusCode"]
        errcode = err.response["Error"]["Code"]
        if status == 404:
            logging.warning("Missing object, %s", errcode)
        elif status == 403:
            logging.error("Access denied, %s", errcode)
        else:
            logging.exception("Error in request, %s", errcode)

s3 = get_s3_client()
SQLALCHEMY_DATABASE_URL="postgresql://train:Ankara06@postgres:5432/traindb"
engine = create_engine(SQLALCHEMY_DATABASE_URL, echo=True)

df = load_df_from_s3(bucket='dataops-bronze',key='raw/dirty_store_transactions.csv', s3=s3)

df2 = df
float_cols = ["MRP", "CP", "DISCOUNT", "SP"]
pattern_text = r"[^A-Za-z0-9\s,.-]"  # for string columns
pattern_num = r"[^0-9.]"

df2["PRODUCT_ID"] = (
    df2["PRODUCT_ID"]
    .astype(str)
    .str.replace(pattern_num, "", regex=True)
    .replace("", np.nan)
    .astype("Int64")
)

for c in float_cols:
    df2[c] = (
        df2[c]
        .astype(str)
        .str.replace(pattern_num, "", regex=True)
        .replace("", np.nan)
        .astype(float)
    )

for c in df2.columns:
    if df2[c].dtype == object and c not in float_cols:
        df2[c] = (
            df2[c]
            .str.replace(pattern_text, "", regex=True)
            .str.strip()
        )

print(df2.head())

df2.to_sql('dirty_store_transactions',con=engine, if_exists='append')