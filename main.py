from fastapi import FastAPI
from pydantic import BaseModel
import ast
import pandas as pd
import re
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
import pydantic


app = FastAPI()

with open(r'steam_games.json') as file:

    data = []
    for line in file.readlines():
      
      data.append(ast.literal_eval(line))

data = pd.DataFrame(data)


def format_date(fecha):
    patron = r'^\d{4}-\d{2}-\d{2}$'
    
    return re.match(patron, str(fecha)) is not None

data = data[data['release_date'].apply(format_date)]
data.release_date = pd.to_datetime(data.release_date)

@app.get("/genero/{fecha}")
def genero(fecha:int):
   
   return {'Géneros': data[data['release_date'].dt.year == fecha]['genres'].value_counts().head(5).index.tolist()}

        

@app.get("/juegos/{fecha}")
def juegos(fecha:int):
   
   return {'Juegos':data[data['release_date'].dt.year == fecha]['title'].unique().tolist()}


@app.get("/specs/{fecha}")
def specs(fecha:int):
   
   return {'Specs':data[data['release_date'].dt.year  == fecha].specs.value_counts().head(5).tolist()}


@app.get("/earlyacces/{fecha}")
def earlyacces(fecha:int):
   
   return {'Acceso Temprano':int(data[data['release_date'].dt.year == fecha].early_access.value_counts()[True])}
   
def meta_value(valor):
    if isinstance(valor, str):
        return None
    else:
        return valor
    
data.metascore = data.metascore.apply(meta_value)

@app.get("/metascore/{fecha}")
def metascore(fecha:int):
    return {'MetaScore':data[data['release_date'].dt.year == fecha].sort_values('metascore', ascending=False).head(5)['title'].tolist()}


@app.get("/sentiment/{fecha}")
def sentiment(fecha:int):
    
    return {clave: valor for clave, valor in data[data['release_date'].dt.year == fecha]['sentiment'].value_counts().to_dict().items() if "user" not in clave or "reviews" not in clave}

data_steam = pd.read_csv('data_steam.csv')

X = data_steam.drop(columns=['price'])
Y = data_steam['price']

X_Train, X_Test, Y_Train, Y_Test = train_test_split(X, Y, test_size = 0.25, random_state=0)
multi_regress = LinearRegression()
trained_regress= multi_regress.fit(X_Train, Y_Train)

Y_Pred = trained_regress.predict(X_Test)

class Prediction(BaseModel):
    release_date: int
    early_access: bool
    metascore: float
    action: int
    adventure: int
    animation_modeling: int
    audio_production: int
    casual: int
    design_illustration: int
    Early_Access : int
    education: int
    indie: int
    massively_multiplayer: int
    photo_editing: int
    rpg: int
    racing: int
    simulation: int
    software_training: int
    sports: int
    strategy: int
    utilities: int
    video_production: int
    web_publishing: int
    captions_available: int
    co_op: int
    commentary_available: int
    cross_platform_multiplayer: int
    downloadable_content: int
    full_controller_support: int
    game_demo: int
    in_app_purchases: int
    includes_source_sdk: int
    includes_level_editor: int
    local_co_op: int
    local_multi_player: int
    mmo: int
    multi_player: int
    online_co_op: int
    online_multi_player: int
    partial_controller_support: int
    shared_split_screen: int
    single_player: int
    stats: int
    steam_achievements: int
    steam_cloud: int
    steam_leaderboards: int
    steam_trading_cards: int
    steam_turn_notifications: int
    steam_workshop: int
    steamvr_collectibles: int
    valve_anti_cheat_enabled: int

@app.post("/prediction")
def predict(Prediction:Prediction):
    
    prediction_dict = Prediction.model_dump()

    X_Train.loc[[0]] = list(prediction_dict.values())

    y_pred = trained_regress.predict(X_Train.loc[[0]])


    return {'Estimación de precio':round(y_pred[0],2), 'RMSE':mean_squared_error(Y_Test, Y_Pred, squared=False)}


