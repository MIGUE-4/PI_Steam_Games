from fastapi import FastAPI
from pydantic import BaseModel
import ast
import pandas as pd
import re
import pydantic


app = FastAPI()

endpoint_developer = pd.read_parquet('data_games.parquet')
data_australian_items = pd.read_parquet('user_items.parquet')
data_australian_reviews = pd.read_parquet('users_reviews.parquet')

def format_date(fecha):
    patron = r'^\d{4}-\d{2}-\d{2}$'
    
    return re.match(patron, str(fecha)) is not None

endpoint_developer = endpoint_developer[endpoint_developer['release_date'].apply(format_date)]

endpoint_developer['release_date'] = pd.to_datetime(endpoint_developer['release_date'], errors='coerce')


endpoint_developer['year'] = endpoint_developer['release_date'].dt.year
endpoint_developer['month'] = endpoint_developer['release_date'].dt.month
endpoint_developer['day'] = endpoint_developer['release_date'].dt.day
def games_total(id):
    juegos_user = []
    
    
    for datos_games in data_australian_items[data_australian_items['user_id']==id]['items'][0]:
        juegos_user.append(datos_games['item_name'])

    data_game_filtrado = endpoint_developer[endpoint_developer['title'].isin(juegos_user)]

    data_game_filtrado['pagado'] = data_game_filtrado['price']-data_game_filtrado['discount_price']

    enumerr_recommeds = []
    for rec in data_australian_reviews[data_australian_reviews['user_id']=='evcentric'].reviews:
        for nums in rec:
            enumerr_recommeds.append(dict(nums).get('recommend'))

    return {'Suma de pagos':data_game_filtrado.pagado, 'porcentaje recomendados':(enumerr_recommeds.count(True)/len(enumerr_recommeds))*100 ,'Cantidad Items':len(juegos_user)}

@app.get("/userdata/{User_id}")
def genero(User_id:str):
   
   return games_total(User_id)

    
def dev_name(developer : str):
    
    juegos_valor_cero = endpoint_developer[(endpoint_developer['developer'] == developer) & (endpoint_developer['price'] == 0)]

    agrupado = juegos_valor_cero.groupby('year')['developer'].count().reset_index(name='juegos_valor_cero')
    total_juegos_por_anio = endpoint_developer[endpoint_developer['developer'] == developer].groupby('year')['developer'].count().reset_index(name='total_juegos')

    resultado = pd.merge(agrupado, total_juegos_por_anio, on='year')
    resultado['Contenido Free'] = (resultado.juegos_valor_cero/resultado.total_juegos)*100
    resultado.drop(columns=['juegos_valor_cero', 'total_juegos'], inplace=True)

    return resultado.to_dict()

@app.get("/developer/{name}")
def earlyacces(name:str):
   
   return dev_name(name)
   
