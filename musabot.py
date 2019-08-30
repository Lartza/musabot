#!/usr/bin/env python3
import time
import audioop
import subprocess as sp
import os
import sys
import logging
from functools import partial
from datetime import timedelta
from urllib.parse import urlparse, parse_qs
from collections import deque

from configobj import ConfigObj

from peewee import Model, TextField, IntegrityError, DoesNotExist, fn
from playhouse.sqlite_ext import SqliteExtDatabase

from googleapiclient.discovery import build
from isodate import parse_duration

import requests

import pymumble.pymumble_py3 as pymumble

from musabot import utils

here = os.path.abspath(os.path.dirname(__file__))
get_path = partial(os.path.join, here)

config = ConfigObj('config.ini')

loglevel = config['loglevel']
numeric_level = getattr(logging, loglevel.upper(), None)
if not isinstance(numeric_level, int):
    raise ValueError('Invalid log level: %s' % loglevel)
logging.basicConfig(level=numeric_level)

filedir = config['filedir']
if not os.path.exists(filedir):
    logging.info("File directory does not exist, creating")
    os.makedirs(filedir)

db = SqliteExtDatabase('musabot.db')


class BaseModel(Model):
    class Meta:
        database = db


class Video(BaseModel):
    id = TextField(primary_key=True)
    url = TextField()
    title = TextField()


db.connect()
Video.create_table(True)
db.close()


def is_admin(user):
    if user['hash'] == config['owner']:
        logging.info(f"{user['name']}({user['hash']}) authenticated as owner")
        return 2
    if user['hash'] in config.as_list('admins'):
        logging.info(f"{user['name']}({user['hash']}) authenticated as admin")
        return 1
    logging.info(f"Failed to authenticate {user['name']}({user['hash']})")
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
                                 developerKey=config['youtube_apikey'], cache_discovery=False)
        else:
            logging.warning('YouTube API Key not set')
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
        logging.debug(f"<{text.actor}> {message}")

        if message.startswith('!'):
            self.handle_command(text, message)

    def launch_play_file(self, video):
        self.stop()
        file = os.path.join(filedir, video['id'])
        if 'starttime' in video:
            command = ["ffmpeg", '-v', 'error', '-nostdin', '-ss', str(video['starttime']), '-i', file,
                       '-ac', '1', '-f', 's16le', '-ar', '48000', '-af', 'loudnorm', '-']
        else:
            command = ["ffmpeg", '-v', 'error', '-nostdin', '-i', file, '-ac', '1', '-f', 's16le',
                       '-ar', '48000', '-af', 'loudnorm', '-']
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

    def send_msg(self, target, msg):
        logging.debug(f"<musabot> -> <{target}> {msg}")
        self.mumble.users[target].send_text_message(msg)

    def send_msg_channel(self, msg, channel=None):
        if not channel:
            try:
                channel = self.mumble.channels[self.mumble.users.myself['channel_id']]
            except KeyError:
                channel = self.mumble.channels[0]
        logging.debug(f"{channel.name} <musabot> {msg}")
        channel.send_text_message(msg)

    def playnext(self):
        self.stop()
        if self.queue:
            logging.debug("Playing track from queue")
            self.current_track = self.queue.popleft()
            self.launch_play_file(self.current_track)
        elif config.as_bool('random'):
            logging.debug("Playing random track")
            self.random()
        else:
            logging.debug("Playback stopped")
            self.playing = False

    def handle_command(self, text, message):
        # TODO timeout
        if self.mumble.users[text.actor]['hash'] in config.as_list('ignored'):
            self.send_msg(text.actor, 'You are on my ignore list')
            return

        if is_admin(self.mumble.users[text.actor]) == 0:
            if config.as_bool('same_channel') and self.mumble.users.myself['channel_id'] != self.mumble.users[text.actor]['channel_id']:
                self.send_msg(text.actor, 'You need to be on the same channel!')
                return
            elif config.as_bool('ignore_private') and text.session:
                if text.session[0] == self.mumble.users.myself['session']:
                    self.send_msg(text.actor, "It's rude to whisper in a group")
                    return

        try:
            command, parameter = message[1:].split(' ', 1)
        except ValueError:
            command = message[1:]
            parameter = None

        if command in ['yt', 'y']:
            self.cmd_youtube(text, parameter)
        elif command in ['vol', 'v']:
            self.cmd_volume(text, parameter)
        elif hasattr(self, 'cmd_' + command):
            getattr(self, 'cmd_' + command)(text, parameter)
        else:
            self.send_msg(text.actor, 'Command {} does not exist'.format(command))

    def play_or_queue(self, video):
        if self.playing:
            logging.debug("Track appended to queue")
            self.queue.append(video)
        else:
            logging.debug("Playing requested track")
            self.current_track = video
            self.launch_play_file(self.current_track)

    def random(self, amount=1):
        db.connect()
        for row in Video.select().order_by(fn.Random()).limit(amount):
            video = {'id': row.id, 'url': row.url, 'title': row.title}
            self.play_or_queue(video)
        db.close()

    def cmd_random(self, text, parameter):
        if parameter is not None:
            amount = int(parameter)
        else:
            amount = 1
        if 1 <= amount <= 10:
            self.send_msg(text.actor, 'Adding {} videos to the queue'.format(amount))
            self.random(amount)

    def cmd_join(self, text, _):
        self.mumble.users.myself.move_in(self.mumble.users[text.actor]['channel_id'])

    def cmd_stop(self, *_):
        self.stop()

    def cmd_play(self, text, _):
        if not self.playing:
            self.playnext()
        else:
            self.send_msg(text.actor, 'I am already playing. Maybe use !skip instead?')

    def cmd_skip(self, *_):
        self.playnext()

    def cmd_np(self, text, _):
        if self.playing:
            self.send_msg(text.actor, 'np: {}'.format(self.current_track['title']))
        else:
            self.send_msg(text.actor, 'Stopped')

    def cmd_youtube(self, text, parameter):
        if config['youtube_apikey'] is not None:
            if parameter is not None:
                url, urlhash = utils.parse_parameter(parameter)
                if urlhash in self.processing:
                    self.send_msg(text.actor, 'Already processing this video!')
                    return
                self.processing.append(urlhash)
                try:
                    db.connect()
                    video_entry = Video.get(Video.id == urlhash)
                    video = {'id': video_entry.id, 'url': video_entry.url, 'title': video_entry.title}
                    db.close()
                except DoesNotExist:
                    db.close()
                    if urlhash in config.as_list('blacklist'):
                        self.send_msg(text.actor, 'Video blacklisted')
                        self.processing.remove(urlhash)
                        return
                    try:
                        videoid = utils.get_yt_video_id(url)
                    except ValueError:
                        self.send_msg(text.actor, 'Invalid YouTube link')
                        self.processing.remove(urlhash)
                        return
                    video = self.download_youtube(text, url, urlhash, videoid)
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
                self.send_msg(text.actor, 'No video given')
        else:
            self.send_msg(text.actor, 'YouTube API Key not set')

    def cmd_mp3(self, text, parameter):
        if parameter is not None:
            url, urlhash = utils.parse_parameter(parameter)
            if urlhash in self.processing:
                self.send_msg(text.actor, 'Already processing this video!')
                return
            self.processing.append(urlhash)
            try:
                db.connect()
                video_entry = Video.get(Video.id == urlhash)
                video = {'id': video_entry.id, 'url': video_entry.url, 'title': video_entry.title}
                db.close()
            except DoesNotExist:
                db.close()
                if urlhash in config.as_list('blacklist'):
                    self.send_msg(text.actor, 'Video blacklisted')
                    self.processing.remove(urlhash)
                    return
                video = self.download_mp3(text, url, urlhash)
                if video is None:
                    return
            self.processing.remove(video['id'])
            self.play_or_queue(video)
        else:
            self.send_msg(text.actor, 'No video given')

    def cmd_queue(self, text, _):
        if self.queue:
            self.send_msg(text.actor, '{} tracks in queue'.format(len(self.queue)))
        else:
            self.send_msg(text.actor, 'No tracks in queue')
    cmd_numtracks = cmd_queue

    def cmd_volume(self, text, parameter):
        if (parameter is not None and parameter.isdigit() and
                0 <= int(parameter) <= 100):
            self.volume = float(float(parameter) / 100)
            config['volume'] = self.volume
            config.write()
            self.send_msg_channel('Vol: {}% by {}'.format(
                int(self.volume * 100), self.mumble.users[text.actor]['name']))
        else:
            self.send_msg(text.actor, 'Volume: {}%'.format(int(self.volume * 100)))

    def download_youtube(self, text, url, urlhash, videoid):
        request = self.youtube.videos().list(part='snippet, contentDetails', id=videoid)
        response = request.execute()
        if parse_duration(response['items'][0]['contentDetails']['duration']) > timedelta(hours=1):
            self.send_msg(text.actor, 'Video too long')
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
            self.send_msg(text.actor, 'Error downloading video')
            self.processing.remove(video['id'])
            return None
        dbresult = self.db_create_video(video, text)
        return dbresult

    def download_mp3(self, text, url, urlhash):
        video = {'id': urlhash, 'url': url, 'title': url.split('/')[-1]}
        request = requests.get(video['url'], stream=True)
        with open(video['id'], 'wb') as file:
            for chunk in request.iter_content(chunk_size=1024):
                if chunk:
                    file.write(chunk)
        dbresult = self.db_create_video(video, text)
        return dbresult

    def db_create_video(self, video, text):
        db.connect()
        try:
            Video.create(id=video['id'], url=video['url'], title=video['title'])
            db.close()
            return video
        except IntegrityError:
            db.close()
            os.remove(os.path.join(filedir, video['id']))
            self.send_msg(text.actor, 'Failed to download due to database error.')
            self.processing.remove(video['id'])
            return None

    def cmd_delete(self, text, parameter):
        if is_admin(self.mumble.users[text.actor]) > 0:
            video = None
            resume = False
            if parameter is not None:
                url, urlhash = utils.parse_parameter(parameter)
                if urlhash is None:
                    return
                db.connect()
                video = Video.get(Video.id == urlhash)
            else:
                if self.playing:
                    resume = True
                    db.connect()
                    logging.debug("Database connection opened")
                    logging.debug("Selecting currently playing track for deletion")
                    video = Video.get(Video.id == self.current_track['id'])
                    self.stop()
                else:
                    self.send_msg(text.actor, 'No video defined')
            if video is not None:
                os.remove(os.path.join(filedir, video.id))
                logging.debug(f"Removed video file {video.id}")
                video.delete_instance()
                logging.debug("Removed database entry for video")
                db.close()
                logging.debug("Database connection closed")
                self.send_msg(text.actor, 'Deleted succesfully')
            else:
                db.close()
                self.send_msg(text.actor, 'Failed to delete')
            if resume:
                logging.debug("Resuming playback")
                self.playnext()

    def cmd_blacklist(self, text, parameter):
        if is_admin(self.mumble.users[text.actor]) > 0:
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
                    self.playnext()
            if video is not None:
                os.remove(os.path.join(filedir, video.id))
                video.delete_instance()
                db.close()
                blacklist = config.as_list('blacklist')
                blacklist.append(video.id)
                config['blacklist'] = blacklist
                config.write()
                self.send_msg(text.actor, 'Blacklisted succesfully')
            else:
                self.send_msg(text.actor, 'Failed to blacklist')

    def cmd_unblacklist(self, text, parameter):
        if is_admin(self.mumble.users[text.actor]) > 0 and parameter:
            url, urlhash = utils.parse_parameter(parameter)
            blacklist = config.as_list('blacklist')
            if urlhash in blacklist:
                blacklist.remove(urlhash)
                config['blacklist'] = blacklist
                config.write()
                self.send_msg(text.actor, "Blacklist removal successful")

    def cmd_togglerandom(self, text, _):
        togglerandom = config.as_bool('random')
        if togglerandom:
            config['random'] = False
            self.send_msg(text.actor, 'Random playback stopped')
        else:
            config['random'] = True
            self.send_msg(text.actor, 'Random playback started')
            if not self.playing:
                self.random()
        config.write()

    def cmd_hash(self, text, parameter):
        if parameter and is_admin(self.mumble.users[text.actor]) == 2:
            for session in self.mumble.users:
                if self.mumble.users[session]['name'] == parameter:
                    self.send_msg(text.actor, self.mumble.users[session]['hash'])
                    return
        else:
            self.send_msg(text.actor, self.mumble.users[text.actor]['hash'])

    def cmd_admin(self, text, parameter):
        if is_admin(self.mumble.users[text.actor]) == 2 and parameter:
            for session in self.mumble.users:
                if self.mumble.users[session]['name'] == parameter:
                    user = self.mumble.users[session]
                    admins = config.as_list('admins')
                    if is_admin(user) != 2 and user['hash'] not in admins:
                        admins.append(user['hash'])
                    config['admins'] = admins
                    config.write()
                    break

    def cmd_unadmin(self, text, parameter):
        if is_admin(self.mumble.users[text.actor]) == 2 and parameter:
            for session in self.mumble.users:
                if self.mumble.users[session]['name'] == parameter:
                    user = self.mumble.users[session]
                    admins = config.as_list('admins')
                    admins.remove(user['hash'])
                    config['admins'] = admins
                    config.write()
                    break

    def cmd_ignore(self, text, parameter):
        if is_admin(self.mumble.users[text.actor]) > 0 and parameter:
            for session in self.mumble.users:
                if self.mumble.users[session]['name'] == parameter:
                    user = self.mumble.users[session]
                    ignored = config.as_list('ignored')
                    if is_admin(user) != 2 and user['hash'] not in ignored:
                        ignored.append(user['hash'])
                    config['ignored'] = ignored
                    config.write()
                    self.send_msg(text.actor, "{}({}) added to ignore list".format(user['name'], user['session']))
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
                    self.send_msg(text.actor, "{}({}) removed from ignore list".format(user['name'], user['session']))
                    break

    def cmd_set(self, text, parameter):
        if is_admin(self.mumble.users[text.actor]) > 0 and parameter:
            parameter = parameter.split(' ', 1)
            value = bool(parameter[1])
            if parameter[0] == 'ignore_private':
                config['ignore_private'] = value
            elif parameter[1] == 'same_channel':
                config['same_channel'] = value
            config.write()
            self.send_msg(text.actor, "Config value set")

    def cmd_kill(self, text, parameter):
        if is_admin(self.mumble.users[text.actor]) > 0:
            self.stop()
            self.exit = True


if __name__ == '__main__':
    musabot = Musabot()
