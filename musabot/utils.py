import hashlib
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup


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

    if query.hostname is None:
        raise ValueError('No hostname found')

    if 'youtube' in query.hostname:
        if query.path == '/watch':
            return parse_qs(query.query)['v'][0]
        if query.path.startswith(('/embed/', '/v/')):
            return query.path.split('/')[2]
    elif 'youtu.be' in query.hostname:
        return query.path[1:]
    raise ValueError('Not a YouTube URL')


def parse_parameter(parameter):
    """Extracts an url from a HTML a element, returns it and it's sha256 hash"""
    soup = BeautifulSoup(parameter, "html.parser")
    try:
        url = soup.find('a').get('href')
    except AttributeError:
        url = parameter

    urlhash = hashlib.sha256(url.encode('utf-8')).hexdigest()

    return url, urlhash


def parse_command(message):
    try:
        command, parameter = message[1:].split(' ', 1)
    except ValueError:
        command = message[1:]
        parameter = None
    return command, parameter


def parse_timecode(url):
    starttime = None

    try:
        timecode = parse_qs(urlparse(url).query)['t'][0]
        starttime = 0
        if 'h' in timecode:
            hours, timecode = timecode.split('h', 1)
            starttime += int(hours) * 3600
        if 'm' in timecode:
            minutes, timecode = timecode.split('m', 1)
            starttime += int(minutes) * 60
        if 's' in timecode:
            seconds, timecode = timecode.split('s', 1)
            starttime += int(seconds)
        if timecode:
            starttime = int(timecode)
    except KeyError:
        pass

    return starttime
