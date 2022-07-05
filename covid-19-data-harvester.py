import requests
import pandas as pd
import io
import os
import smtplib
from email.message import EmailMessage
# import numpy as np

pd.options.mode.chained_assignment = None  # default="warn"

# Indicateurs par département
url = "https://www.data.gouv.fr/fr/datasets/r/5c4e1452-3850-4b59-b11c-3dd51d7fb8b5"
response = requests.get(url)
response.encoding = "UTF-8"

# Colonnes
# "dep","date","reg","lib_dep","lib_reg","tx_pos","tx_incid","TO","R","hosp","rea","rad","dchosp","reg_rea","incid_hosp","incid_rea","incid_rad","incid_dchosp","reg_incid_rea","pos","pos_7j","cv_dose1"
df_raw = pd.read_csv(io.StringIO(response.text), dtype={"dep": "str"}, encoding="UTF-8")


# Sort dataframe by date
df_raw["date"] = pd.to_datetime(df_raw.date)
df_raw.sort_values(by=["date","dep"], inplace=True, ascending = [True, True])

df_tx_incid = df_raw.dropna(subset=["tx_incid"])
df_TO = df_raw.dropna(subset=["TO"])
df_tx_pos = df_raw.dropna(subset=["tx_pos"])
df_R = df_raw.dropna(subset=["R"])

dfo_tx_incid = df_tx_incid.drop(["reg","lib_reg","tx_pos","TO","R","hosp","rea","rad","dchosp","reg_rea","incid_hosp","incid_rea","incid_rad","incid_dchosp","reg_incid_rea","pos","pos_7j","cv_dose1"], axis=1)
dfo_tx_incid.rename(columns = {"tx_incid":"valeur"}, inplace = True)
output_tx_incid = dfo_tx_incid.to_csv("data/dep-spf-tx_incid.csv", index=False, line_terminator="\n")

dfo_TO = df_TO.drop(["reg","lib_reg","tx_pos","tx_incid","R","hosp","rea","rad","dchosp","reg_rea","incid_hosp","incid_rea","incid_rad","incid_dchosp","reg_incid_rea","pos","pos_7j","cv_dose1"], axis=1)
dfo_TO.rename(columns = {"TO":"valeur"}, inplace = True)
output_TO = dfo_TO.to_csv("data/dep-spf-TO.csv", index=False, line_terminator="\n")

dfo_tx_pos = df_tx_pos.drop(["reg","lib_reg","tx_incid","TO","R","hosp","rea","rad","dchosp","reg_rea","incid_hosp","incid_rea","incid_rad","incid_dchosp","reg_incid_rea","pos","pos_7j","cv_dose1"], axis=1)
dfo_tx_pos.rename(columns = {"tx_pos":"valeur"}, inplace = True)
output_tx_pos = dfo_tx_pos.to_csv("data/dep-spf-tx_pos.csv", index=False, line_terminator="\n")

dfo_R = df_R.drop(["reg","lib_reg","tx_pos","tx_incid","TO","hosp","rea","rad","dchosp","reg_rea","incid_hosp","incid_rea","incid_rad","incid_dchosp","reg_incid_rea","pos","pos_7j","cv_dose1"], axis=1)
dfo_R.rename(columns = {"R":"valeur"}, inplace = True)
output_R = dfo_R.to_csv("data/dep-spf-R.csv", index=False, line_terminator="\n")


# Indicateurs France
url = "https://www.data.gouv.fr/fr/datasets/r/f335f9ea-86e3-4ffa-9684-93c009d5e617"
response = requests.get(url)
response.encoding = "UTF-8"

# Colonnes
# "date","tx_pos","tx_incid","TO","R","rea","hosp","rad","dchosp","incid_rea","incid_hosp","incid_rad","incid_dchosp","conf","conf_j1","pos","esms_dc","dc_tot","pos_7j","cv_dose1","esms_cas"
df_raw = pd.read_csv(io.StringIO(response.text), encoding="UTF-8")



# Sort dataframe by date
df_raw["date"] = pd.to_datetime(df_raw.date)
df_raw.sort_values(by=["date"], inplace=True, ascending = [True])

df_tx_incid = df_raw.dropna(subset=["tx_incid"])
df_TO = df_raw.dropna(subset=["TO"])
df_tx_pos = df_raw.dropna(subset=["tx_pos"])
df_R = df_raw.dropna(subset=["R"])

# Example file
dfo_example = df_tx_incid.drop(["rea","hosp","rad","dchosp","incid_rea","incid_hosp","incid_rad","incid_dchosp","conf","conf_j1","pos","esms_dc","dc_tot","pos_7j","cv_dose1","esms_cas"], axis=1)
dfo_example.rename(columns = {"date":"Date", "tx_incid":"Taux d'incidence", "tx_pos":"Taux de positivité des tests", "R":"Facteur de reproduction du virus", "TO":"Tension hospitalière"}, inplace = True)
dfo2_example = dfo_example.drop(dfo_example.index[20:])
output_sample = dfo2_example.to_csv("data/sample.csv", index=False, line_terminator="\n")


dfo_tx_incid = df_tx_incid.drop(["tx_pos","TO","R","rea","hosp","rad","dchosp","incid_rea","incid_hosp","incid_rad","incid_dchosp","conf","conf_j1","pos","esms_dc","dc_tot","pos_7j","cv_dose1","esms_cas"], axis=1)
dfo_tx_incid.rename(columns = {"tx_incid":"valeur"}, inplace = True)
dfo_latest_tx_incid = dfo_tx_incid.iloc[dfo_tx_incid["date"].argmax():]
dfo_latest_tx_incid.rename(columns = {"tx_incid":"valeur"}, inplace = True)
output_tx_incid = dfo_tx_incid.to_csv("data/france-spf-tx_incid.csv", index=False, line_terminator="\n")
output_latest_tx_incid = dfo_latest_tx_incid.to_csv("data/france-spf-latest-tx_incid.csv", index=False, line_terminator="\n")

dfo_TO = df_TO.drop(["tx_pos","tx_incid","R","rea","hosp","rad","dchosp","incid_rea","incid_hosp","incid_rad","incid_dchosp","conf","conf_j1","pos","esms_dc","dc_tot","pos_7j","cv_dose1","esms_cas"], axis=1)
dfo_TO.rename(columns = {"TO":"valeur"}, inplace = True)
dfo_latest_TO = dfo_TO.iloc[dfo_TO["date"].argmax():]
dfo_latest_TO.rename(columns = {"TO":"valeur"}, inplace = True)
output_TO = dfo_TO.to_csv("data/france-spf-TO.csv", index=False, line_terminator="\n")
output_latest_TO = dfo_latest_TO.to_csv("data/france-spf-latest-TO.csv", index=False, line_terminator="\n")

dfo_tx_pos = df_tx_pos.drop(["tx_incid","TO","R","rea","hosp","rad","dchosp","incid_rea","incid_hosp","incid_rad","incid_dchosp","conf","conf_j1","pos","esms_dc","dc_tot","pos_7j","cv_dose1","esms_cas"], axis=1)
dfo_tx_pos.rename(columns = {"tx_pos":"valeur"}, inplace = True)
dfo_latest_tx_pos = dfo_tx_pos.iloc[dfo_tx_pos["date"].argmax():]
dfo_latest_tx_pos.rename(columns = {"tx_pos":"valeur"}, inplace = True)
output_tx_pos = dfo_tx_pos.to_csv("data/france-spf-tx_pos.csv", index=False, line_terminator="\n")
output_latest_tx_pos = dfo_latest_tx_pos.to_csv("data/france-spf-latest-tx_pos.csv", index=False, line_terminator="\n")

dfo_R = df_R.drop(["tx_pos","tx_incid","TO","rea","hosp","rad","dchosp","incid_rea","incid_hosp","incid_rad","incid_dchosp","conf","conf_j1","pos","esms_dc","dc_tot","pos_7j","cv_dose1","esms_cas"], axis=1)
dfo_R.rename(columns = {"R":"valeur"}, inplace = True)
dfo_latest_R = dfo_R.iloc[dfo_R["date"].argmax():]
dfo_latest_R.rename(columns = {"R":"valeur"}, inplace = True)
output_R = dfo_R.to_csv("data/france-spf-R.csv", index=False, line_terminator="\n")
output_latest_R = dfo_latest_R.to_csv("data/france-spf-latest-R.csv", index=False, line_terminator="\n")


# Get mail informations
EMAIL_SENDER = os.environ.get('EMAIL_SENDER')
EMAIL_PASSWORD = os.environ.get('EMAIL_PASSWORD')
EMAIL_RECIPIENT = os.environ.get('EMAIL_RECIPIENT')
SMTP_SERVER = os.environ.get('SMTP_SERVER')
SMTP_PORT = os.environ.get('SMTP_PORT')

# Write mail content
msg = EmailMessage()
msg['Subject'] = 'GitHub Actions: covid-19-data-france'
msg['From'] = EMAIL_SENDER
msg['To'] = EMAIL_RECIPIENT
msg.set_content('Actualisation terminée.')

# Send mail
with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as smtp:
    smtp.login(EMAIL_SENDER, EMAIL_PASSWORD)
    smtp.send_message(msg)
