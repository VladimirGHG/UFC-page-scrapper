import asyncio
import threading

import asyncpg
import requests
from bs4 import BeautifulSoup


class Threads(threading.Thread):
    def __init__(self, name):
        super().__init__()
        self.name = name.replace(" ", "-")
        self.wins, self.opponents, self.dates, self.round_counts = [], [], [], []
        self.times, self.methods = [], []
        self.data_lists = [self.round_counts, self.times, self.methods]

    def run(self):
        res = requests.get(f"https://www.ufc.com/athlete/{self.name}").text
        soup = BeautifulSoup(res, "lxml")
        fighter_records = soup.find("div", attrs={"class", "athlete-record"}).find_all("li",
                                                                                       attrs={
                                                                                           "class": "l-listing__item"})
        for record in fighter_records:
            # Scrapping who won
            r = record.find(
                "div",
                attrs={"class",
                       "c-card-event--athlete-results__matchup"})
            check_draw = r.find("div",
                                attrs={"class",
                                       "c-card-event--athlete-results__plaque win"})
            if check_draw:
                prev = check_draw.find_previous()
                if self.name in prev.find("a").get('href'):
                    self.wins.append("Win")

                else:
                    self.wins.append("Defeat")

            else:
                self.wins.append("Draw")

            # Scrapping opponent names
            path_to_opponent = (record.find("h3", attrs={"class": "c-card-event--athlete-results__headline"}).
                                find_all("a"))
            for sides in path_to_opponent:
                if self.name not in sides.get("href"):
                    opponent = sides.get("href")[28:]
                    self.opponents.append(opponent)

            # Scrapping match dates
            self.dates.append(record.find("div", attrs={"class": "c-card-event--athlete-results__date"}).text)

            # Scrapping round counts, time, and method
            rounds_time_method = record.find_all("div", attrs={"class": "c-card-event--athlete-results__result-text"})
            if not rounds_time_method:
                for j in range(len(self.data_lists)):
                    self.data_lists[j].append("No data")
            else:
                for idx, data in enumerate(rounds_time_method):
                    self.data_lists[idx].append(data.text)


if __name__ == "__main__":
    names = input("Enter athlete names you want to get "
                  "info about by separating their full names with coma").lower().split(",")

    threads = [Threads(name) for name in names]

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


    async def create_database():

        user = 'postgres'
        password = ''
        host = 'localhost'
        port = 5432
        database_name = 'my_db'

        conn = await asyncpg.connect(user=user, password=password, host=host, port=port, database='postgres')

        db_exists = await conn.fetchval("SELECT 1 FROM pg_database WHERE datname=$1", database_name)

        if not db_exists:
            await conn.execute(f'CREATE DATABASE {database_name}')
            print(f"Database '{database_name}' created successfully.")
        else:
            print(f"Database '{database_name}' already exists.")

        await conn.close()


    async def setup_database():

        user = 'postgres'
        password = ''
        host = 'localhost'
        port = 5432
        database_name = 'my_db'

        conn = await asyncpg.connect(user=user, password=password, host=host, port=port, database=database_name)

        await conn.execute('''
            CREATE TABLE IF NOT EXISTS athletes_data (
                id SERIAL PRIMARY KEY,
                athlete TEXT,
                competitors TEXT,
                results TEXT,
                time TEXT,
                round_counts TEXT,
                methods TEXT,
                dates TEXT
            )''')

        number_of_athletes = len(threads)
        for g in range(number_of_athletes):
            for j in range(len(threads[g].opponents)):
                await conn.execute('''
                    INSERT INTO athletes_data (athlete, competitors, results, time, round_counts, methods, dates) VALUES
                    ($1, $2, $3, $4, $5, $6, $7)''', threads[g].name, threads[g].opponents[j],
                                   threads[g].wins[j], threads[g].times[j],
                                   threads[g].round_counts[j], threads[g].methods[j],
                                   threads[g].dates[j])

        await conn.close()


    asyncio.run(create_database())
    asyncio.run(setup_database())
