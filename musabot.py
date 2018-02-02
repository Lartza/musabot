#!/usr/bin/env python3
import time
import audioop
import subprocess as sp
import os
from functools import partial
import pymumble.pymumble_py3 as pymumble
from configobj import ConfigObj
from peewee import Model, TextField, IntegrityError, DoesNotExist
from playhouse.sqlite_ext import SqliteExtDatabase
import hashlib
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup
import subprocess
from googleapiclient.discovery import build
from isodate import parse_duration
from datetime import timedelta

here = os.path.abspath(os.path.dirname(__file__))
get_path = partial(os.path.join, here)

db = SqliteExtDatabase('musabot.db')


class BaseModel(Model):
    class Meta:
        database = db


class Video(BaseModel):
    id = TextField(unique=True)
    url = TextField()
    title = TextField()


db.connect()
Video.create_table(True)
db.close()


class MyLogger(object):
    def debug(self, msg):
        print(msg)

    def warning(self, msg):
        print(msg)

    def error(self, msg):
        print(msg)


def get_yt_video_id(url):
    """Returns Video_ID extracting from the given url of Youtube

    Examples of URLs:
      Valid:
        'http://youtu.be/_lOT2p_FCvA',
        'www.youtube.com/watch?v=_lOT2p_FCvA&feature=feedu',
        'http://www.youtube.com/embed/_lOT2p_FCvA',
        'http://www.youtube.com/v/_lOT2p_FCvA?version=3&amp;hl=en_US',
        'https://www.youtube.com/watch?v=rTHlyTphWP0&index=6&list=PLjeDyYvG6-40qawYNR4juzvSOg-ezZ2a6',
        'youtube.com/watch?v=_lOT2p_FCvA',

      Invalid:
        'youtu.be/watch?v=_lOT2p_FCvA',
    """

    if url.startswith(('youtu', 'www')):
        url = 'http://' + url

    query = urlparse(url)

    if 'youtube' in query.hostname:
        if query.path == '/watch':
            return parse_qs(query.query)['v'][0]
        elif query.path.startswith(('/embed/', '/v/')):
            return query.path.split('/')[2]
    elif 'youtu.be' in query.hostname:
        return query.path[1:]
    else:
        raise ValueError


def parse_parameter(parameter):
    soup = BeautifulSoup(parameter, "html.parser")
    try:
        url = soup.find('a').get('href')
    except AttributeError:
        url = parameter

    urlhash = hashlib.sha256(url.encode('utf-8')).hexdigest()

    return url, urlhash


class Musabot:
    def __init__(self):
        self.config = ConfigObj('config.ini')
        self.volume = self.config.as_float('volume')
        self.filedir = self.config['filedir']

        self.playing = False
        self.exit = False
        self.thread = None

        self.logger = MyLogger()

        self.processing = []
        self.current_track = None

        if self.config['youtube_apikey']:
            self.youtube = build('youtube', 'v3',
                                 developerKey=self.config['youtube_apikey'])
        else:
            print('YouTube API Key not set')
            self.youtube = None

        self.mumble = pymumble.Mumble(self.config['host'], self.config['user'], port=self.config.as_int('port'),
                                      password=self.config['password'], certfile=self.config['cert'],
                                      keyfile=self.config['privkey'], reconnect=True)
        self.mumble.callbacks.set_callback("text_received", self.message_received)
        self.mumble.set_codec_profile("audio")
        self.mumble.start()
        self.mumble.is_ready()
        self.mumble.set_bandwidth(200000)
        self.loop()

    def message_received(self, text):
        message = text.message.strip()

        self.logger.debug('{}: {}'.format(self.mumble.users[text.actor]['name'], message))

        if message.startswith('!'):
            self.handle_command(text, message)

    def is_admin(self, user):
        if user['hash'] == self.config['owner']:
            return 2
        if user['hash'] in self.config.as_list('admins'):
            return 1
        return 0

    def launch_play_file(self, video):
        self.stop()
        file = os.path.join(self.filedir, video['id'])
        if 'starttime' in video:
            command = ["ffmpeg", '-v', 'error', '-nostdin', '-ss', str(video['starttime']), '-i', file,
                       '-ac', '1', '-f', 's16le', '-ar', '48000', '-']
        else:
            command = ["ffmpeg", '-v', 'error', '-nostdin', '-i', file, '-ac', '1', '-f', 's16le',
                       '-ar', '48000', '-']
        self.thread = sp.Popen(command, stdout=sp.PIPE, bufsize=480)
        self.playing = True

    def loop(self):
        while not self.exit and self.mumble.isAlive():
            if self.playing:
                while self.mumble.sound_output.get_buffer_size() > 0.5 and self.playing:
                    time.sleep(0.01)
                raw_music = self.thread.stdout.read(480)
                if raw_music:
                    self.mumble.sound_output.add_sound(audioop.mul(raw_music, 2, self.volume))
                else:
                    time.sleep(0.01)
            else:
                time.sleep(1)

        while self.mumble.sound_output.get_buffer_size() > 0:
            time.sleep(0.01)
        time.sleep(0.5)

    def stop(self):
        if self.thread:
            self.playing = False
            time.sleep(0.5)
            self.thread.kill()
            self.thread = None
            self.current_track = None

    def send_msg_channel(self, msg, channel=None):
        if not channel:
            try:
                channel = self.mumble.channels[self.mumble.users.myself['channel_id']]
            except KeyError:
                channel = self.mumble.channels[0]
        channel.send_text_message(msg)

    def handle_command(self, text, message):
        if self.mumble.users[text.actor]['hash'] in self.config.as_list('ignored'):
            self.mumble.users[text.actor].send_message('You are on my ignore list')
            return

        try:
            command, parameter = message[1:].split(' ', 1)
        except ValueError:
            command = message[1:]
            parameter = None

        if command in ['joinme', 'join', 'come', 'j']:
            self.mumble.users.myself.move_in(self.mumble.users[text.actor]['channel_id'])
        elif command in ['youtube', 'yt', 'y']:
            self.cmd_youtube(text, parameter)

    def cmd_youtube(self, text, parameter):
        if self.config['youtube_apikey'] is not None:
            if parameter is not None:
                url, urlhash = parse_parameter(parameter)
                if urlhash in self.processing:
                    self.mumble.users[text.actor].send_message('Already processing this video!')
                    return
                self.processing.append(urlhash)
                try:
                    db.connect()
                    video_entry = Video.get(Video.id == urlhash)
                    video = {'id': video_entry.id, 'url': video_entry.url, 'title': video_entry.title}
                    db.close()
                except DoesNotExist:
                    db.close()
                    try:
                        videoid = get_yt_video_id(url)
                    except ValueError:
                        self.mumble.users[text.actor].send_message('Invalid YouTube link')
                        self.processing.remove(urlhash)
                        return
                    request = self.youtube.videos().list(part='snippet, contentDetails', id=videoid)
                    response = request.execute()
                    if parse_duration(response['items'][0]['contentDetails']['duration']) > timedelta(hours=1):
                        self.mumble.users[text.actor].send_message('Video too long')
                        self.processing.remove(urlhash)
                        return
                    video = {'id': urlhash, 'url': url, 'title': response['items'][0]['snippet']['title']}
                    try:
                        subprocess.run(
                            'youtube-dl -f best --no-playlist -4 -o "{}/{}.%(ext)s" --extract-audio --audio-format mp3 --audio-quality 2 -- {}'.format(
                                self.filedir, video['id'], videoid), shell=True, check=True)
                        os.rename(os.path.join(self.filedir, '{}.mp3'.format(video['id'])),
                                  os.path.join(self.filedir, video['id']))
                    except subprocess.CalledProcessError:
                        self.mumble.users[text.actor].send_message('Error downloading video')
                        self.processing.remove(video['id'])
                        return
                    db.connect()
                    try:
                        Video.create(id=video['id'], url=video['url'], title=video['title'])
                        db.close()
                    except IntegrityError:
                        db.close()
                        os.remove(os.path.join(self.filedir, video['id']))
                        self.mumble.users[text.actor].send_message('Failed to download due to database error.')
                        self.processing.remove(video['id'])
                        return
                try:
                    timecode = parse_qs(urlparse(video['url']).query)['t'][0]
                    video['starttime'] = 0
                    if 'h' in timecode:
                        hours, timecode = timecode.split('h', 1)
                        video['starttime'] += int(hours) * 3600
                    if 'm' in timecode:
                        minutes, timecode = timecode.split('m', 1)
                        video['starttime'] += int(minutes) * 60
                    if 's' in timecode:
                        seconds, timecode = timecode.split('s', 1)
                        video['starttime'] += int(seconds)
                    if timecode:
                        video['starttime'] = int(timecode)
                except KeyError:
                    pass
                self.processing.remove(video['id'])
                self.launch_play_file(video)
            else:
                self.mumble.users[text.actor].send_message('No video given')
        else:
            self.mumble.users[text.actor].send_message('YouTube API Key not set')


if __name__ == '__main__':
    musabot = Musabot()
