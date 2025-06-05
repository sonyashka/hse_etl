import pandas as pd
import numpy as np

df = pd.read_csv('mental_health.csv')
# df_tmp = df['care_options']
# print(df.describe())

# кандидаты на преобразование в bool
# self_employed, family_history, treatment
# coping_struggles

df['self_employed'] = np.where(df['self_employed'] == 'Yes', 'true', 'false')
df['family_history'] = np.where(df['family_history'] == 'Yes', 'true', 'false')
df['treatment'] = np.where(df['treatment'] == 'Yes', 'true', 'false')
df['Coping_Struggles'] = np.where(df['Coping_Struggles'] == 'Yes', 'true', 'false')
# print(df.head())

df['Timestamp'] = pd.to_datetime(df['Timestamp']).dt.date.astype(str)

# df['Timestamp'] = "(DATE('" + df['Timestamp'] + "')"
# df['Gender'] = "'" + df['Gender'] + "'"
# df['Country'] = "'" + df['Country'] + "'"
# df['Occupation'] = "'" + df['Occupation'] + "'"
# df['Days_Indoors'] = "'" + df['Days_Indoors'] + "'"
# df['Growing_Stress'] = "'" + df['Growing_Stress'] + "'"
# df['Changes_Habits'] = "'" + df['Changes_Habits'] + "'"
# df['Mental_Health_History'] = "'" + df['Mental_Health_History'] + "'"
# df['Mood_Swings'] = "'" + df['Mood_Swings'] + "'"
# df['Work_Interest'] = "'" + df['Work_Interest'] + "'"
# df['Social_Weakness'] = "'" + df['Social_Weakness'] + "'"
# df['mental_health_interview'] = "'" + df['mental_health_interview'] + "'"
# df['care_options'] = "'" + df['care_options'] + "')"

# print(df.describe())

df = df.assign(rn=list(range(1, len(df)+1)))
df = df[:100000]
print(df.shape[0])
df.to_csv('mh_transformed.csv', index=False, columns=['rn','Timestamp','Gender','Country','Occupation','self_employed','family_history','treatment','Days_Indoors','Growing_Stress','Changes_Habits','Mental_Health_History','Mood_Swings','Coping_Struggles','Work_Interest','Social_Weakness','mental_health_interview','care_options'])