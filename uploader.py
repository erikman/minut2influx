#!/usr/bin/env python3

import asyncio
import argparse
from datetime import datetime, timezone, timedelta
import json
import os
from time import time
from typing import Optional
from dateutil import parser as dateparser

import aiohttp
from dotenv import load_dotenv
from influxdb import InfluxDBClient


class EnvDefault(argparse.Action):
    # pylint: disable=redefined-builtin
    def __init__(self, envvar, required=False, default=None, help: str = '', **kwargs):
        if envvar in os.environ:
            default = os.environ[envvar]

        help += ' env:' + envvar
        if required and default:
            required = False
        super(EnvDefault, self).__init__(default=default, required=required, help=help,
                                         **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)


def time_to_iso8601(time_point: datetime):
    # assert(dt.timetz == timezone.utc)
    return time_point.strftime('%Y-%m-%dT%H:%M:%S.000Z')


def minut_to_datetime(value) -> datetime:
    if isinstance(value, int):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    elif isinstance(value, str):
        return dateparser.isoparse(value)
    else:
        raise ValueError(f'Unknown time type ({value})')


class MinutSession:
    def __init__(self,
                 session: aiohttp.ClientSession,
                 client_id: str,
                 client_secret: str,
                 redirect_uri: Optional[str] = None):
        self.session = session
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri

        self.headers = {
            'Accept': 'application/json'
        }

        self.access_token = None
        self.token_type = None
        self.refresh_token = None
        self.expires_in = 0

    def get_state(self):
        return {
            'access_token': self.access_token,
            'token_type':  self.token_type,
            'refresh_token': self.refresh_token,
            'expires_in': self.expires_in
        }

    def set_state(self, state):
        self.access_token = state['access_token']
        self.token_type = state['token_type']
        self.refresh_token = state['refresh_token']
        self.expires_in = state['expires_in']

        self._update_headers()

    def store_state(self, file_name='state.json'):
        with open(file_name, 'w') as state_file:
            json.dump(self.get_state(), state_file)

    def load_state(self, file_name='state.json'):
        with open(file_name, 'r') as state_file:
            state = json.load(state_file)
            self.set_state(state)

    def _update_headers(self):
        self.headers['Authorization'] = f'{self.token_type} {self.access_token}'

    async def do_auth(self, authorization_code: Optional[str] = None,
                      username: Optional[str] = None,
                      password: Optional[str] = None,
                      refresh_token: Optional[str] = None):
        body = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
        }

        if authorization_code is not None:
            body['grant_type'] = 'authorization_code'
            body['redirect_uri'] = self.redirect_uri
            body['code'] = authorization_code
        elif refresh_token is not None:
            body['grant_type'] = 'refresh_token'
            body['refresh_token'] = refresh_token
        else:
            body['grant_type'] = 'password'
            body['username'] = username
            body['password'] = password

        headers = {
            'Content-Type': 'application/json',
            'Cache-Control': 'no-cache'
        }

        async with self.session.post('https://api.minut.com/v1/oauth/token',
                                     json=body, headers=headers) as resp:
            if resp.status != 200:
                print('{} authorization failed'.format(body['grant_type']))

                self.access_token = None
                self.token_type = None
                self.refresh_token = None
                self.expires_in = 0

                return False

            auth = await resp.json()

            self.access_token = auth['access_token']
            self.token_type = auth['token_type']
            self.refresh_token = auth['refresh_token']
            self.expires_in = time() + auth['expires_in']

            self._update_headers()
            return True

    def has_access_token(self):
        return self.access_token is not None and time() < self.expires_in

    def has_refresh_token(self):
        return self.refresh_token is not None

    async def do_refresh_token(self):
        if time() < self.expires_in:
            return True

        return await self.do_auth(refresh_token=self.refresh_token)

    async def get_devices(self):
        async with self.session.get('https://api.minut.com/v1/devices',
                                    headers=self.headers) as resp:
            return await resp.json()

    async def _get_value_from_device(self, device_id: str, endpoint: str,
                                     start_at: datetime, end_at: datetime):
        params = {
            'start_at': time_to_iso8601(start_at),
            'end_at': time_to_iso8601(end_at),
            'raw': 'true',
            'format': 'compact',
            'include_min_max': 'false'
        }
        async with self.session.get(f'https://api.minut.com/v1/devices/{device_id}/{endpoint}',
                                    headers=self.headers, params=params) as resp:
            data = await resp.json()
            return [(minut_to_datetime(timestamp), float(value))
                    for (timestamp, value) in data['values']]

    async def get_pressure(self, device_id: str, start_at: datetime, end_at: datetime):
        return await self._get_value_from_device(device_id, 'pressure', start_at, end_at)

    async def get_sound_level(self, device_id: str, start_at: datetime, end_at: datetime):
        return await self._get_value_from_device(device_id, 'sound_level', start_at, end_at)

    async def get_temperature(self, device_id: str, start_at: datetime, end_at: datetime):
        return await self._get_value_from_device(device_id, 'temperature', start_at, end_at)

    async def get_humidity(self, device_id: str, start_at: datetime, end_at: datetime):
        return await self._get_value_from_device(device_id, 'humidity', start_at, end_at)


def month_sequence(start: datetime, end: datetime = datetime.now(tz=timezone.utc), step=1):
    download_start = start
    while download_start < end:
        download_end = (download_start + timedelta(days=31) *
                        step).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        yield (download_start, download_end)

        download_start = download_end


async def measurement_sequence(device_id: str,
                               start_time: datetime,
                               end_time: datetime,
                               measurement: str,
                               get,
                               state: dict):

    for (download_start, download_end) in month_sequence(start_time, end_time):
        values = await get(device_id, start_at=download_start, end_at=download_end)

        # Reverse the values so we can pop efficiently
        values.reverse()

        # Prepare data for influx
        while len(values) > 0:
            (timestamp, value) = values.pop()
            if timestamp > end_time:
                break

            yield (timestamp, device_id, measurement, value)

            # Update state with last downloaded timestamp
            if not 'devices' in state:
                state['devices'] = {}
            if not device_id in state['devices']:
                state['devices'][device_id] = {}
            state['devices'][device_id][measurement] = timestamp.isoformat()


async def merge_sequences(generators: list):
    async def get_or_none(generator):
        """Return next value from an async generator or None at end of sequence"""
        try:
            return await generator.__anext__()
        except StopAsyncIteration:
            return None

    # Fill initial values from generators
    values = [await get_or_none(generator) for generator in generators]

    def timestamp_for_index(i: int):
        if values[i] is not None:
            return values[i][0]
        return datetime.max.replace(tzinfo=timezone.utc)

    # Process all values
    while any(map(lambda value: value is not None, values)):
        # Find next value to return
        min_index = min(range(len(values)), key=timestamp_for_index)

        yield values[min_index]
        values[min_index] = await get_or_none(generators[min_index])


def dict_get(values: dict, key: str):
    """Retrieve deep key in dict of dicts

       Example:
       d = {
           'a': {
               'b': {
                   'c': 10
               }
           }
       }

       dict_get(d, 'a.b.c') = 10

       Return None if not found
    """
    keys = key.split('.')
    for k in keys:
        if isinstance(values, dict) and k in values:
            values = values[k]
        else:
            return None
    return values

async def download_devices(influx_client: InfluxDBClient,
                           minut: MinutSession,
                           state: dict,
                           start_time: Optional[datetime] = None,
                           end_time: Optional[datetime] = None):

    getters = [
        ('humidity', minut.get_humidity),
        ('temperature', minut.get_temperature),
        ('sound_level', minut.get_sound_level),
        ('pressure', minut.get_pressure),
    ]

    devices = await minut.get_devices()
    first_start_time = datetime.max.replace(tzinfo=timezone.utc)
    if end_time is None:
        end_time = datetime.now(tz=timezone.utc)

    # Collect generators to download from
    generators = []
    for device in devices['devices']:
        device_id = device['device_id']

        # Retrieve last download time from state object
        for measurement, getter in getters:
            measurement_start_time = dateparser.isoparse(device['first_seen_at'])
            if start_time is not None:
                measurement_start_time = max(measurement_start_time, start_time)
            else:
                already_downloaded = dict_get(state, f'devices.{device_id}.{measurement}')
                if already_downloaded is not None:
                    measurement_start_time = dateparser.isoparse(already_downloaded)

            first_start_time = min(first_start_time, measurement_start_time)

            generators.append(measurement_sequence(device_id,
                                                   measurement_start_time, end_time,
                                                   measurement,
                                                   getter,
                                                   state))

    # Sort the data from all generators before passing onto influx
    influx_data = []
    async for timestamp, device_id, measurement, value in merge_sequences(generators):
        influx_data.append(
            {
                'measurement': measurement,
                'tags': {
                    'device': device_id,
                },
                'time':  timestamp.isoformat(),
                'fields': {
                    'value': float(value)
                }
            }
        )

        if len(influx_data) >= 40960:
            progress = (timestamp - first_start_time) / \
                (end_time - first_start_time)
            print(f'Uploading... ({progress:.0%})', flush=True)

            influx_client.write_points(influx_data, time_precision='s')
            influx_data = []

    if len(influx_data) > 0:
        influx_client.write_points(influx_data, time_precision='s')

async def download(influx_client: InfluxDBClient, args):
    async with aiohttp.ClientSession() as session:
        minut = MinutSession(session, client_id=args.minut_client_id,
                             client_secret=args.minut_client_secret,
                             redirect_uri=args.minut_redirect_uri)

        state = {}
        try:
            with open(args.state_path, 'r') as state_file:
                state = json.load(state_file)
                minut.set_state(state)
        except OSError:
            pass

        if minut.has_refresh_token():
            await minut.do_refresh_token()

        if not minut.has_access_token():
            if args.minut_authorization_code is not None:
                await minut.do_auth(authorization_code=args.minut_authorization_code)
            elif args.minut_username is not None and args.minut_password is not None:
                await minut.do_auth(username=args.minut_username, password=args.minut_password)
            else:
                raise Exception('Can\'t authenticate with minut, '
                                'need authorization code or username/password')

        if not minut.has_access_token():
            raise Exception('Authentication with minut failed')

        await download_devices(influx_client, minut=minut, state=state,
                               start_time=args.start_time, end_time=args.end_time)

        updated_state = {**state, **minut.get_state()}
        with open(args.state_path, 'w') as state_file:
            json.dump(updated_state, state_file)


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(
        description='Upload data from Minut.com to influx database')

    parser.add_argument(
        '--state-path', action=EnvDefault, envvar='MINUT_STATE_PATH',
        help='Path to file storing state')

    parser.add_argument(
        '--minut-authorization-code', action=EnvDefault, envvar='MINUT_AUTH_CODE',
        help='Minut authorization token')
    parser.add_argument(
        '--minut-username', action=EnvDefault, envvar='MINUT_USERNAME',
        help='Minut username')
    parser.add_argument(
        '--minut-password', action=EnvDefault, envvar='MINUT_PASSWORD',
        help='Minut password')
    parser.add_argument(
        '--minut-client-id', action=EnvDefault, envvar='MINUT_CLIENT_ID', required=True,
        help='Minut client id')
    parser.add_argument(
        '--minut-client-secret', action=EnvDefault, envvar='MINUT_CLIENT_SECRET', required=True,
        help='Minut client secret')
    parser.add_argument(
        '--minut-redirect-uri', action=EnvDefault, envvar='MINUT_REDIRECT_URI',
        default='http://localhost:8000', help='Minut redirect uri')

    parser.add_argument(
        '--influx-host', action=EnvDefault, envvar='INFLUX_HOST', default='localhost',
        help='Influx host')
    parser.add_argument(
        '--influx-port', action=EnvDefault, envvar='INFLUX_PORT', default=8086, type=int,
        help='Influx port')
    parser.add_argument(
        '--influx-user', action=EnvDefault, envvar='INFLUX_USER', default='root',
        help='Influx username')
    parser.add_argument(
        '--influx-password', action=EnvDefault, envvar='INFLUX_PASSWORD', default='root',
        help='Influx password')
    parser.add_argument(
        '--influx-database', action=EnvDefault, envvar='INFLUX_DATABASE', default='minut',
        help='Influx database')

    parser.add_argument(
        '--start-time', type=dateparser.isoparse,
        help='First date to download (also ignores previously downloaded state)'
    )
    parser.add_argument(
        '--end-time', type=dateparser.isoparse, help='Last date to download'
    )

    args = parser.parse_args()
    influx_client = InfluxDBClient(host=args.influx_host, port=args.influx_port,
                                   username=args.influx_user, password=args.influx_password,
                                   database=args.influx_database)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(download(influx_client, args))


if __name__ == '__main__':
    main()
