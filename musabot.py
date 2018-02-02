#!/usr/bin/env python3
import time
import audioop
import subprocess as sp
import os
from functools import partial
from datetime import timedelta
from urllib.parse import urlparse, parse_qs
from collections import deque

from configobj import ConfigObj

from peewee import Model, TextField, IntegrityError, DoesNotExist, fn
from playhouse.sqlite_ext import SqliteExtDatabase

from googleapiclient.discovery import build
from isodate import parse_duration

import pymumble.pymumble_py3 as pymumble

from musabot import utils

here = os.path.abspath(os.path.dirname(__file__))
get_path = partial(os.path.join, here)

config = ConfigObj('config.ini')
filedir = config['filedir']
if not os.path.exists(filedir):
    os.makedirs(filedir)

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


def is_admin(user):
    if user['hash'] == config['owner']:
        return 2
    if user['hash'] in config.as_list('admins'):
        return 1
    return 0


class Musabot:
    def __init__(self):
        self.volume = config.as_float('volume')

        self.playing = False
        self.exit = False
        self.thread = None

        self.processing = []
        self.current_track = None
        self.queue = deque()

        if config['youtube_apikey']:
            self.youtube = build('youtube', 'v3',
                                 developerKey=config['youtube_apikey'])
        else:
            print('YouTube API Key not set')
            self.youtube = None

        self.mumble = pymumble.Mumble(config['host'], config['user'], port=config.as_int('port'),
                                      password=config['password'], certfile=config['cert'],
                                      keyfile=config['privkey'], reconnect=True)
        self.mumble.callbacks.set_callback("text_received", self.message_received)
        self.mumble.set_codec_profile("audio")
        self.mumble.start()
        self.mumble.is_ready()
        self.mumble.set_bandwidth(200000)
        self.loop()

    def message_received(self, text):
        message = text.message.strip()

        if message.startswith('!'):
            self.handle_command(text, message)

    def launch_play_file(self, video):
        self.stop()
        file = os.path.join(filedir, video['id'])
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
                    self.playnext()
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

    def playnext(self):
        self.stop()
        if self.queue:
            self.current_track = self.queue.popleft()
            self.launch_play_file(self.current_track)
        elif config.as_bool('random'):
            self.random()
        else:
            self.playing = False

    def handle_command(self, text, message):
        if self.mumble.users[text.actor]['hash'] in config.as_list('ignored'):
            self.mumble.users[text.actor].send_message('You are on my ignore list')
            return

        try:
            command, parameter = message[1:].split(' ', 1)
        except ValueError:
            command = message[1:]
            parameter = None

        if command in ['yt', 'y']:
            self.cmd_youtube(text, parameter)
        elif hasattr(self, 'cmd_' + command):
            getattr(self, 'cmd_' + command)(text, parameter)
        else:
            self.mumble.users[text.actor].send_message('Command {} does not exist'.format(command))

    def play_or_queue(self, video):
        if self.playing:
            self.queue.append(video)
        else:
            self.current_track = video
            self.launch_play_file(self.current_track)

    def random(self, amount=1):
        db.connect()
        for row in Video.select().order_by(fn.Random()).limit(amount):
            video = {'id': row.id, 'url': row.url, 'title': row.title}
            self.play_or_queue(video)
        db.close()

    def cmd_random(self, _, parameter):
        if parameter is not None:
            amount = int(parameter)
        else:
            amount = 1
        if 1 <= amount <= 10:
            self.random(amount)

    def cmd_join(self, text, _):
        self.mumble.users.myself.move_in(self.mumble.users[text.actor]['channel_id'])

    def cmd_stop(self, *_):
        self.stop()

    def cmd_play(self, text, _):
        if not self.playing:
            self.playnext()
        else:
            self.mumble.users[text.actor].send_message('I am already playing. Maybe use !skip instead?')

    def cmd_skip(self, *_):
        self.playnext()

    def cmd_np(self, text, _):
        if self.playing:
            self.mumble.users[text.actor].send_message('np: {}'.format(self.current_track['title']))
        else:
            self.mumble.users[text.actor].send_message('Stopped')

    def cmd_youtube(self, text, parameter):
        if config['youtube_apikey'] is not None:
            if parameter is not None:
                url, urlhash = utils.parse_parameter(parameter)
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
                    video = self.download_youtube(text, url, urlhash)
                    if video is None:
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
                self.play_or_queue(video)
            else:
                self.mumble.users[text.actor].send_message('No video given')
        else:
            self.mumble.users[text.actor].send_message('YouTube API Key not set')

    def cmd_queue(self, text, _):
        if self.queue:
            self.mumble.users[text.actor].send_message('{} tracks in queue'.format(len(self.queue)))
        else:
            self.mumble.users[text.actor].send_message('No tracks in queue')
    cmd_numtracks = cmd_queue

    def cmd_volume(self, text, parameter):
        if (parameter is not None and parameter.isdigit() and
                0 <= int(parameter) <= 100):
            self.volume = float(float(parameter) / 100)
            config['volume'] = self.volume
            config.write()
            self.send_msg_channel('Volume: {}% set by {}'.format(
                int(self.volume * 100), self.mumble.users[text.actor]['name']))
        else:
            self.mumble.users[text.actor].send_message(
                'Volume: {}%'.format(int(self.volume * 100)))

    def download_youtube(self, text, url, urlhash):
        try:
            videoid = utils.get_yt_video_id(url)
        except ValueError:
            self.mumble.users[text.actor].send_message('Invalid YouTube link')
            self.processing.remove(urlhash)
            return None
        request = self.youtube.videos().list(part='snippet, contentDetails', id=videoid)
        response = request.execute()
        if parse_duration(response['items'][0]['contentDetails']['duration']) > timedelta(hours=1):
            self.mumble.users[text.actor].send_message('Video too long')
            self.processing.remove(urlhash)
            return None
        video = {'id': urlhash, 'url': url, 'title': response['items'][0]['snippet']['title']}
        try:
            sp.run(
                'youtube-dl -f best --no-playlist -4 -o "{}/{}.%(ext)s" --extract-audio --audio-format mp3 --audio-quality 2 -- {}'
                .format(filedir, video['id'], videoid), shell=True, check=True)
            os.rename(os.path.join(filedir, '{}.mp3'.format(video['id'])),
                      os.path.join(filedir, video['id']))
        except sp.CalledProcessError:
            self.mumble.users[text.actor].send_message('Error downloading video')
            self.processing.remove(video['id'])
            return None
        db.connect()
        try:
            Video.create(id=video['id'], url=video['url'], title=video['title'])
            db.close()
        except IntegrityError:
            db.close()
            os.remove(os.path.join(filedir, video['id']))
            self.mumble.users[text.actor].send_message('Failed to download due to database error.')
            self.processing.remove(video['id'])
            return None
        return video

    def delete(self, text, parameter):
        video = None
        if parameter is not None:
            url, urlhash = utils.parse_parameter(parameter)
            if urlhash is None:
                return
            db.connect()
            video = Video.get(Video.id == urlhash)
        else:
            if self.playing:
                video = Video.get(Video.id == self.current_track['id'])
        if video is not None:
            self.stop()
            os.remove(video.filename)
            video.delete_instance()
            db.close()
            self.mumble.users[text.actor].send_message('Deleted succesfully')
            self.playnext()
        else:
            self.mumble.users[text.actor].send_message('Failed to delete')

    def cmd_togglerandom(self, text, _):
        togglerandom = config.as_bool('random')
        if togglerandom:
            config['random'] = False
            self.mumble.users[text.actor].send_message('Random playback stopped')
        else:
            config['random'] = True
            self.mumble.users[text.actor].send_message('Random playback started')
            if not self.playing:
                self.random()
        config.write()

    def cmd_hash(self, text, parameter):
        if parameter and is_admin(self.mumble.users[text.actor]) == 2:
            for session in self.mumble.users:
                if self.mumble.users[session]['name'] == parameter:
                    self.mumble.users[text.actor].send_message(self.mumble.users[session]['hash'])
                    return
        else:
            self.mumble.users[text.actor].send_message(self.mumble.users[text.actor]['hash'])

    def cmd_ignore(self, text, parameter):
        if is_admin(self.mumble.users[text.actor]) > 0 and parameter:
            for session in self.mumble.users:
                if self.mumble.users[session]['name'] == parameter:
                    user = self.mumble.users[session]
                    ignored = config.as_list('ignored')
                    if is_admin(user) != 2:
                        ignored.append(user['hash'])
                    config['ignored'] = ignored
                    config.write()
                    break

    def cmd_unignore(self, text, parameter):
        if is_admin(self.mumble.users[text.actor]) > 0 and parameter:
            for session in self.mumble.users:
                if self.mumble.users[session]['name'] == parameter:
                    user = self.mumble.users[session]
                    ignored = config.as_list('ignored')
                    ignored.remove(user['hash'])
                    config['ignored'] = ignored
                    config.write()
                    break

    # TODO: mp3, samechannel, noprivate


if __name__ == '__main__':
    musabot = Musabot()
