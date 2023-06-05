import aiohttp
import asyncio
import pandas as pd
import json

async def fetch(session, id):
    url = f"https://statsapi.web.nhl.com/api/v1/people/{id}"
    async with session.get(url) as response:
        player_data = await response.json()
        if response.status == 200:
            date_of_birth = player_data['people'][0]['birthDate']
            return pd.to_datetime(date_of_birth).date()
        else:
            print(f"Failed to get data, status code: {response.status}")
            return None

async def main():
    skater_by_year = []
    for ptype in ['skaters', 'goalies']:
        for year in range(2008, 2023):
            df = pd.read_csv(f'./data/raw/{year}{ptype}.csv')
            player_ids = df['playerId']
            async with aiohttp.ClientSession() as session:
                tasks = []
                for id in player_ids:
                    tasks.append(fetch(session, id))
                date_of_births = await asyncio.gather(*tasks)
                df['age'] = year - pd.Series(date_of_births).apply(lambda x: pd.Timestamp(x).year)
                df['date_of_birth'] = date_of_births
                print(f'Finished {year}')
                skater_by_year.append(df)

        skater_by_year = pd.concat(skater_by_year)
        skater_by_year.to_csv(f'./data/processed/{ptype}.csv', index=False)


asyncio.run(main())