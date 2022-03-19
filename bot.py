from datetime import time, timezone
import logging

import configparser
import pymysql
import pymysql.cursors

import discord
from discord.ext import tasks

import ao_chart

# Set up logging
logging.basicConfig(filename='./logs/bot.log',
                            filemode = 'a',
                            format='%(asctime)s %(levelname)-8s %(message)s',
                            datefmt = '%Y-%m-%d %H:%M:%S',
                            level = logging.INFO)

class DiscordBot(discord.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Setup a DB connection for each region
        self.mydb = pymysql.connect(
            host=kwargs['host'],
            port=kwargs['port'],
            user=kwargs['user'],
            password=kwargs['password'],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)

        # TODO: read all the regions, 1st-f channel from the database
        self.regions = {
            'f3pugetsound': 954511419548790804,
        }

        # Start the tasks to run in the background
        self.monthly_task.start()

    async def on_ready(self):
        logging.info(f'Logged in as {self.user} (ID: {self.user.id})')

    @tasks.loop(seconds=10000)  # TODO: Task that runs every month
    async def monthly_task(self):
        for (region, channel) in self.regions.items():
            channel = self.get_channel(channel)  # Your Channel ID goes here
            logging.info(f'generating chart for {region}')
            chart_path = ao_chart.ao_monthly_summary_chart(self.mydb, region)
            logging.info(f'sending chart to {channel}')
            await channel.send(
                content='Hey Puget Sound/Seattle - it\'s that time of the month again. Here is a detailed summary of AO posting stats for the region last month.',
                file=discord.File(chart_path)
            )
        # async for message in channel.history(limit=200, oldest_first=True):
        #    print(message.content)

    @monthly_task.before_loop
    async def before_tasks(self):
        await self.wait_until_ready()  # Wait until the bot logs in

config = configparser.ConfigParser()
config.read('./credentials.ini')
host = config['aws']['host']
port = int(config['aws']['port'])
user = config['aws']['user']
password = config['aws']['password']
db = config['aws']['db']

client = DiscordBot(host=host, port=port, user=user, password=password, db=db)
client.run(config['discord']['token'])