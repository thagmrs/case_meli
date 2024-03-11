import pandas as pd

mostrado = pd.read_json((r'..\case_meli\prints.json'), lines=True)
mostrado = mostrado.assign(
    position=mostrado['event_data'].apply(lambda x: x['position']),
    value_prop=mostrado['event_data'].apply(lambda x: x['value_prop'])
).drop('event_data', axis=1)


clicado = pd.read_json(r'..\case_meli\taps.json', lines=True)
clicado = (clicado
        .assign(position=clicado['event_data'].apply(lambda x: x['position']),
                value_prop=clicado['event_data'].apply(lambda x: x['value_prop']))
        .drop('event_data', axis=1)
        .assign(clicked=1))

pagamento = pd.read_csv(r'..\case_meli\pays.csv')
pagamento = pagamento.rename(columns={'pay_date': 'day'}) 

df_joined = (
    mostrado
    .merge(pagamento, how='outer', on=['user_id', 'value_prop', 'day'])
    .merge(clicado, how='outer', on=['user_id', 'value_prop', 'day', 'position'])
)

ult_sem = pd.to_datetime(df_joined['day']).max() - pd.Timedelta(days=7)
tres_sem_atras = ult_sem - pd.Timedelta(weeks=3)

prints_ult_sem = df_joined[pd.to_datetime(df_joined['day']) >= ult_sem].copy()
prints_ult_sem['clicked'] = prints_ult_sem['clicked'].fillna(0).astype('int')

tres_sem_atras = df_joined[(pd.to_datetime(df_joined['day']) >= tres_sem_atras) & (pd.to_datetime(df_joined['day']) < ult_sem)]

visualizado = tres_sem_atras.groupby('value_prop')['clicked'].sum().astype(int)

clicado = tres_sem_atras.groupby(['user_id', 'value_prop'])['clicked'].sum().astype(int)

pago = tres_sem_atras.groupby(['user_id','value_prop']).agg({'total': 'count'})

valor_pago = pago = tres_sem_atras.groupby(['user_id','value_prop']).agg({'total': 'sum'})


print('Informações da última semana com flag de clicado ou não')
print(clicado)

print('Informações das 3 semanas anteriores, agrupadas por:')

print('Número de cliques por value prop')
print(visualizado)

print('Número de cliques em cada value prop por usuário')
print(clicado)

print('Número de pagamentos realizados em cada value prop por usuário')
print(pago)

print('valor em pagamentos realizados em cada value prop por usuário')
print(valor_pago)