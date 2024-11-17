import json

import pandas as pd

def clean(df: pd.DataFrame) -> pd.DataFrame:
    df['Category'] = df['Description'].apply(lambda x: x.lower().replace(' ', '').split(',')[1:][0])

    with open('tests/out.json') as file:
        mapping = json.loads(file.read())

    df['Category'] = df['Category'].map(mapping)
    df['Value'] = df['Value'].abs()
    df['Date'] = pd.to_datetime(df['Date'])
    return df


def test_read():

    df = pd.read_csv('tests/transactions.csv')
    df = df[df['Type'] == 'POS']
    types = df['Description'].unique()

    results = []
    mapping = {}

    for index, type in enumerate(types):
        cleaned = type.lower()
        cleaned = cleaned.replace(' ', '').split(',')[1:][0]
        results.append(cleaned)

        mapping[cleaned] = ''

    for index, item in enumerate(sorted(set(results))):
        print(f'{index}: {item} \n')

    # with open('mapping.json', 'w+') as file:
    #     file.write(json.dumps(mapping))

    df = clean(df)



