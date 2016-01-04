#!/usr/bin/python

import json
import re
import psycopg2
import os

import urllib2
import urllib
import time
import logging
import datetime


class Datastore:
    postgres = "dbname=photobucket user=postgres password=xyz"

    def __init__(self):
        self.conn = None
        self.connect()

    def connect(self):
        if self.conn is None or self.conn.closed != 0:
            self.conn = psycopg2.connect(self.postgres)

        if self.conn.status == psycopg2.extensions.STATUS_IN_TRANSACTION:
            self.conn.reset()

        return self.conn

    def store_pb_user(self, username, album_path):
        try:
            cur = self.conn.cursor()
            cur.execute(
                        'BEGIN Insert into pb_user (username, profile_link) values '
                        '( %s,%s ); EXCEPTION WHEN unique_violation then -- do nothing END;',
                        (username, album_path))
            self.conn.commit()
        except Exception, e:
            logging.warn("Exception store_pb_user (insert) : " + str(e))

    def store_links(self, full_size, thumb, username, saved_file, album_path, objs):
        try:
            cur = self.conn.cursor()
            cur.execute(
                        'Insert into links (link, thumb, username, filename, albumpath, obj, last_date) values '
                        '( %s,%s,%s,%s,%s,%s, current_timestamp );',
                        (full_size, thumb, username, saved_file, album_path, objs))
            self.conn.commit()
        except Exception, e:
            logging.warn("Exception store_links (insert) : " + str(e))

    def find_file(self, username, filename, partial=False):
        try:
            cur = self.conn.cursor()
            if partial:
                cur.execute("select filename from links where username = %s and filename like %s",
                            (username, filename))
            else:
                cur.execute("select filename from links where username = %s and filename = %s",
                            (username, filename))
            return cur.fetchone()
        except Exception, e:
            logging.warn("Exception find_filename (insert) : " + str(e))
            return None

    def skip_user(self, username):
        try:
            cur = self.conn.cursor()
            cur.execute("Select * from skip_user where username = %s;", (username,))
            results = cur.fetchone()
            if results is not None:
                logging.info("---> Skipping user " + username)
                return True
        except Exception, e:
            logging.warn("Exception skip_user " + str(e))

        return False

    def add_skipped_user(self, username):
        try:
            if self.skip_user(username):
                return
            else:
                cur = self.conn.cursor()
                count = cur.execute("insert into skip_user (username) values ( '%s' )", username)
                if count != 1:
                    logging.warn("===== Unable to insert skip username " + username + " into table")
        except Exception, e:
            logging.warn("Exception in add_skipped_user" + str(e))


class Utils:

    @staticmethod
    def scan_directory(video_dir):
        if os.path.exists(video_dir):
            if os.path.isdir(video_dir):
                return os.listdir(video_dir)

    @staticmethod
    def rotate_directory(video_dir):
        if os.path.exists(video_dir):
            if os.path.isdir(video_dir):
                os.rename(video_dir,
                          os.path.join("e:/", "temp_" + str(datetime.datetime.now().microsecond)))
        os.mkdir(video_dir)


class Photobucket:
    FORMAT = '%(asctime)-15s %(message)s'
    video_path = "e:\\temp"
    thumb_path = "e:\\temp"
    skip_path = "e:\\skip"

    log_path = "e:/"

    def __init__(self):
        self.db = Datastore()

    def find_filename(self, obj):
        saved = False
        try:
            thumb = obj["thumbUrl"]
            album_path = obj["albumPath"]
            filename = obj['name']
            full_size = obj["fullsizeUrl"]
            username = obj["username"]
            saved_file = username + "!" + filename

            if "xvideo" in filename:
                return saved

            if "_zps" in saved_file:
                filename = saved_file[0:saved_file.index("_zps")] + "%"
                logging.info("_zps found " + filename)
                results = self.db.find_file(username, filename, partial=True)
            else:
                results = self.db.find_file(username, saved_file)

            if results is None:
                filename = os.path.join(self.video_path, saved_file)
                urllib.urlretrieve(full_size, filename)
                # urllib.urlretrieve(thumb, os.path.join(self.thumb_path, saved_file))

                # if the file isn't big enough to be more than a second don't bother to save it
                stats = os.stat(filename)
                if stats.st_size <= 350000:
                    try:
                        logging.info("Removing " + filename + " size = " + str(stats.st_size))
                        os.remove(filename)
                    except IOError:
                        return saved
                else:
                    logging.info("Saving " + filename + " size = " + str(stats.st_size))

                try:
                    self.db.store_links(full_size, thumb, username, saved_file, album_path,  json.dumps(obj))

                except Exception, e:
                    logging.warn("Exception find_filename (insert) : " + str(e))
                saved = True
            else:
                logging.info("Skipping " + saved_file)
        except Exception, e:
            logging.warn("Exception find_filename (general) : " + str(e))

        return saved

    def run(self):
        log_dir = os.path.join(self.log_path, "test.log")
        if os.path.isfile(log_dir):
            os.remove(log_dir)

        video_dir = os.path.join(self.video_path)
        user_dir = os.path.join(self.video_path, "skip")
        win_dir = os.path.join(self.video_path, "win")

        Utils.rotate_directory(video_dir)

        logging.basicConfig(filename=log_dir, level=logging.DEBUG, format=self.FORMAT)
        done = False
        count = 0
        while not done:
            try:
                logging.info("Fetching...")
                url = "http://photobucket.com/recentuploads?filter=video&page=1"
                try:
                    req = urllib2.Request(url=url, data="")
                    f = urllib2.urlopen(req)
                    json_data = f.readlines()
                except Exception, e:
                    logging.warn("Exception fetching " + str(e))
                    continue

                collection = None
                for line in json_data:
                    if re.search('collectionData:', line):
                        collection = line[20:-2]
                        break

                if collection is None:
                    logging.warn("No collection data found on page.")
                    time.sleep(60)
                    continue

                data = json.loads(collection)

                saved = False
                for obj in data["items"]["objects"]:
                    username = obj["username"]

                    if self.db.skip_user(username):
                        continue

                    if self.find_filename(obj):
                        count += 1
                        saved = True

                if count > 3000:
                    Utils.rotate_directory(video_dir)
                    count = 0
                    time.sleep(120)

                if not saved:
                    logging.info("No data saved...waiting for 60 seconds")
                    time.sleep(60)
            except Exception, e:
                logging.info("Exception in wait loop " + str(e))


# with daemon.DaemonContext():
pbucket = Photobucket()
pbucket.run()
