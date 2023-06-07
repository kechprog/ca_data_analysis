import pandas as pd
import asyncio
import aiohttp
from typing import List, Tuple

games = pd.read_csv('./data/processed/games.csv')
async def get_players_in_game(session, game_id: int, team: str) -> Tuple[List, List]:
    team = team.lower()
    url = f'https://statsapi.web.nhl.com/api/v1/game/{game_id}/feed/live'
    
    async with session.get(url) as r:
        data = await r.json()
        players = data['liveData']['boxscore']['teams'][team]
        return players['skaters'], players['goalies']

async def main():
    tasks = []
    connector = aiohttp.TCPConnector(limit=10)  # Limit the number of connections
    async with aiohttp.ClientSession(connector=connector) as session:
        for index, row in games.iterrows():
            task = asyncio.ensure_future(get_players_in_game(session, row['gameId'], row['home_or_away']))
            tasks.append(task)
        
        games['playerIds'] = await asyncio.gather(*tasks)
        games.to_csv('./data/processed/__games.csv', index=False)

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
