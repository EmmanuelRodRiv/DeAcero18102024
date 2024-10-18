from google.cloud.bigquery import _pyarrow_helpers
from google.cloud import bigquery
from google.oauth2 import service_account
import google.cloud.bigquery as bq


credentials = service_account.Credentials.from_service_account_file('C:\Users\kaser\Downloads\DeAcero\EjercicioDeAcero17102024\pythonBigQuery\bankmarketingdataset-privatekey.json')

project_id = 'BankmarketingDataSet'
client = bigquery.Client(credentials= 'erodriguesr11@gmail.com',project='BankmarketingDataSet')

#Crear cte's para para simular unir varias fuentes y obtener las columnas necesarias para el dataMart
query = client.query1("""
    CREATE OR REPLACE TABLE  bankmarketingdataset.bankmarketingdatasetV1.dataMartMarketing  as 
    ( WITH  bank as (
    SELECT  poutcome, age, marital,	education 
      FROM `bankmarketingdataset.bankmarketingdatasetV1.raw_bank_marketing` where poutcome = 'other' )
    ,bank1 as (
    SELECT  poutcome,age, marital,	education 
      FROM `bankmarketingdataset.bankmarketingdatasetV1.raw_bank_marketing` where poutcome = 'failure' )
    ,bank2 as (
    SELECT  poutcome,age, marital,	education 
      FROM `bankmarketingdataset.bankmarketingdatasetV1.raw_bank_marketing` where poutcome = 'success' )
    ,bank3 as (
    SELECT  poutcome, age, marital,	education 
      FROM `bankmarketingdataset.bankmarketingdatasetV1.raw_bank_marketing` where poutcome = 'unknown' )

    SELECT * FROM bank
      UNION ALL
    SELECT * FROM bank1 
      UNION ALL
    SELECT * FROM bank2
      UNION ALL
    SELECT * FROM bank3
    );
 """);
# Distribucion de leads contactados y procentaje de efectividad

query = client.query2("""
SELECT 
  UPPER(poutcome) AS EstatusContacto,
  Count(poutcome) AS TotContactados,
  CONCAT(CAST(ROUND(((Count(poutcome) * 100)  / (SELECT count(*)  
                                                     FROM bankmarketingdataset.bankmarketingdatasetV1.dataMartMarketing)),2) AS STRING),"%")  as PorcEfectividad
FROM bankmarketingdataset.bankmarketingdatasetV1.dataMartMarketing
  GROUP BY poutcome
    ORDER BY Count(poutcome) Desc
     """)
