import asyncio
from datetime import date, datetime, timedelta, time, timezone
import logging
import re

import configparser
import pandas as pd
import pymysql
import pymysql.cursors

import discord
from discord.ext import commands, tasks

import ao_chart

# Set up logging
logging.basicConfig(filename='./logs/bot.log',
                            filemode = 'a',
                            format='%(asctime)s %(levelname)-8s %(message)s',
                            datefmt = '%Y-%m-%d %H:%M:%S',
                            level = logging.INFO)
class RegionMetadata():
    def __init__(self, schema_name, firstf):
        self.schema_name = schema_name
        self.firstf_chid = firstf

class BackblastParseError(Exception):
    def __init__(self, message, *args):
        super().__init__(args)
        self.message = message

class DiscordBot(commands.Bot):
    def __init__(self, *args, **kwargs):
        super().__init__(command_prefix="!")
        # Setup a DB connection for each region
        self.mydb = pymysql.connect(
            host=kwargs['host'],
            port=kwargs['port'],
            user=kwargs['user'],
            password=kwargs['password'],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)

        # TODO: We need to associate each guild (Discord server) with a region (and underlying schema).
        # We should read this from the DB, but we'll hardcode this here for now.
        # Guild ID -> RegionMetadata
        self.regions = {
            688887373102186632: RegionMetadata('f3pugetsound', 900115864760766464)
        }

        # Start the tasks to run in the background
        self.hourly_task.start()
        self.daily_task.start()
        self.monthly_task.start()

    async def on_ready(self):
        logging.info(f'Logged in as {self.user} (ID: {self.user.id})')

    def parse_users(self, value: str, mentions: list[discord.User]):
        # mentions contains the full list of all mentions in the message
        # it may not correspond to the actual attending PAX
        
        # the mention string may be <@1234> or <@!1234> (see https://discordjs.guide/miscellaneous/parsing-mention-arguments.html#how-discord-mentions-work)

        # now, we filter out values for just those mentions
        return [user.id for user in mentions if
            value.find(user.mention) != -1 or
            value.find(user.mention.replace('@', '@!')) != -1]

    def is_zero(self, value):
        if len(value) == 0:
            return True
        if value in ['none', 'None', 'None listed', 'NA', 'zero', '-', '0', '']:
            return True
        return False

    async def parse_backblast(self, msg: discord.Message):
        content = msg.content
        logging.info(f'Parsing backblast {msg.id}')
        # Split the content into lines
        lines = re.split(r'\n', content)
        # Define the fields we care about
        fields = {}
        field_names = set(['Q', 'Count', 'FNGs', 'Date', 'AO', 'PAX'])
        parsed_fields = []

        for line in lines:
            for field in field_names:
                if line.strip().startswith(field + ':'):
                    value = line.strip()[len(field) + 2:].strip()
                    match field:
                        case 'Q':
                            qs = self.parse_users(value, msg.mentions)
                            if len(qs) > 0:
                                fields['q_user_id'] = qs[0]
                                parsed_fields.append(field)
                            if len(qs) > 1:
                                fields['coq_user_id'] = qs[1]
                        case 'Count':
                            try:
                                fields['pax_count'] = int(value)
                                parsed_fields.append(field)
                            except ValueError:
                                raise BackblastParseError('Count was not a valid number')
                        case 'FNGs':
                            fields['fngs'] = value
                            fields['fng_count'] = 0 if self.is_zero(value) else len(value.split(','))
                            parsed_fields.append(field)
                        case 'Date':
                            try:
                                fields['bd_date'] = date.fromisoformat(value)
                                parsed_fields.append(field)
                            except ValueError:
                                raise BackblastParseError('Expecting date to be YYYY-MM-DD format')
                        case 'AO':
                            # This should match a channel we know about
                            # if it starts with a '#' then it's plain text
                            # if it starts with '<' then it's a mention
                            ao_id = None
                            if value.startswith('<'):
                                ao_id = value[2:-1] # drop <# and >
                                # look up to make sure it exists
                                if await self.fetch_channel(ao_id) is None:
                                    raise BackblastParseError('Received a channel I don\'t know about as the AO')
                                
                                fields['ao_id'] = ao_id
                                parsed_fields.append(field)

                            elif value.startswith('#'):
                                raise BackblastParseError('AO should be a channel mention')
                        case 'PAX':
                            fields['pax'] = self.parse_users(value, msg.mentions)
                            parsed_fields.append(field)
                        case _:
                            raise BackblastParseError('Unexpected field in backblast parsing ' + field)
    
        # Check that we have all fields
        missing_fields = field_names.difference(parsed_fields)
        if len(missing_fields) > 0:
            raise BackblastParseError('Missing fields: ' + ', '.join(missing_fields))

        print(fields)
        # return dataframe


    async def mine_channel(self, ch_id, after_time):
        logging.info(f'Searching {ch_id} for backblasts.')
        # We'll any message that starts with 'backblast'
        # (and allow for leading whitespace)
        backblast_re = r'\s*backblast'
        ch = self.get_channel(ch_id)
        async for msg in ch.history(limit=100, after=after_time, oldest_first=True):
            if not msg.author.bot and re.match(backblast_re, msg.content, flags=re.IGNORECASE) is not None:
                try:
                    bb_df = await self.parse_backblast(msg)
                    # TODO: actually insert
                except BackblastParseError as ex:
                    await msg.reply(f'I had a problem parsing the backblast: {ex.message}')
                except BaseException as ex:
                    await msg.reply(f'I had an error recording the backblast: {ex=}, {type(ex)=}')

    async def mine_beatdowns(self):
        for (guild_id, region) in self.regions.items():
            logging.info(f'Mining beatdowns for {region.schema_name}')
            self.mydb.select_db(region.schema_name)
            with self.mydb.cursor() as cursor:
                sql = "SELECT channel_id, ao FROM aos WHERE backblast = 1 AND archived = 0"
                cursor.execute(sql)
                channels = cursor.fetchall()
                channels_df = pd.DataFrame(channels, columns={'channel_id', 'ao'})

            # filter channels to those that are actually on Discord
            channels = [*filter(lambda ch_id: ch_id in self.get_guild(guild_id).text_channels,
                channels_df['channel_id']), 954511419548790804] # bot testing channel

            # There's probably a more Pythonic way to do this, but you cannot
            # use `timedelta` with timezone `datetime`s. But, we do want the
            # time to be UTC-based when passing it to the API.
            one_hour_ago = datetime.utcfromtimestamp(
                (datetime.now() - timedelta(hours=1)).timestamp())
            asyncio.gather(*[self.mine_channel(ch_id, one_hour_ago) for ch_id in channels])

    @tasks.loop(hours=1)
    async def hourly_task(self):
        logging.info('Starting hourly tasks')
        await self.mine_beatdowns()

    async def list_channels(self):
        for guild in self.guilds:
            if guild.id in self.regions:
                logging.info(f'Found schema mapping for {guild.name} ({guild.id}). Populating channels.')
                records = map(lambda c: { 'channel_id': c.id, 'ao': c.name, 'channel_created': c.created_at.timestamp(), 'archived': 0 }, guild.text_channels)
                
                channel_df = pd.DataFrame.from_records(records)
                # TODO: actually insert

    async def list_users(self):
        for guild in self.guilds:
            if guild.id in self.regions:
                logging.info(f'Found schema mapping for {guild.name} ({guild.id}). Populating users.')
                records = map(lambda u: { 'user_id': u.id, 'user_name': u.display_name, 'real_name': u.name}, guild.members)

                users_df = pd.DataFrame.from_records(records)
                # TODO: actually insert

    @tasks.loop(hours=24)
    async def daily_task(self):
        logging.info('Starting daily tasks')
        asyncio.gather(self.list_channels(), self.list_users())

    async def leaderboard_charter(self):
        logging.info(f'Building leaderboard charts')
        for (guild_id, region) in self.regions.items():
            channel = self.get_channel(region.firstf_chid)
            logging.info(f'generating chart for {region.schema_name}')
            chart_path = ao_chart.ao_monthly_summary_chart(self.mydb, region.schema_name)
            logging.info(f'sending chart to {channel}')
            await channel.send(
                content='Hey Puget Sound/Seattle - it\'s that time of the month again. Here is a detailed summary of AO posting stats for the region last month.',
                file=discord.File(chart_path)
            )

    async def pax_charter(self):
        logging.info(f'Building unique PAX charts')

    async def ao_charter(self):
        logging.info(f'Building AO charts')


    # @tasks.loop(time=time(hour=17, minute=0))
    @tasks.loop(hours=24)
    async def monthly_task(self):
        if datetime.utcnow().hour != 17:
            return
        logging.info('Starting monthly tasks')
        asyncio.gather(
            self.leaderboard_charter(),
            self.pax_charter(),
            self.ao_charter()
        )
    
    @hourly_task.before_loop
    @daily_task.before_loop
    @monthly_task.before_loop
    async def before_tasks(self):
        logging.info('Before tasks')
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