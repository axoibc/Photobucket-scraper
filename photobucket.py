import json
import urllib
import urllib2
import re
import logging
import os
import time
import photo_data
import Utils


class Photobucket:
    FORMAT = '%(asctime)-15s %(message)s'
    video_path = "f:\\temp"
    skip_path = "f:\\temp"
    log_path = "f:/"

    def __init__(self, username, password, dbname):
        self.db = photo_data.Data(username, password, dbname)
        self.video_dir = os.path.join(self.video_path, "temp")
        self.user_dir = os.path.join(self.video_path, "skip")
        self.win_dir = os.path.join(self.video_path, "win")
        self.log_dir = os.path.join(self.log_path, "test.log")

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
                filename = saved_file[0:saved_file.index("_zps")+4] + "%"
                logging.info("_zps found " + filename)
                results = self.db.find_file(username, filename, partial=True)
            else:
                results = self.db.find_file(username, saved_file)

            if results is None:
                filename = os.path.join(self.video_dir, saved_file)
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
                logging.info("Duplicate " + saved_file)
        except Exception, e:
            logging.warn("Exception find_filename (general) : " + str(e))

        return saved

    def run(self):
        if os.path.isfile(self.log_dir):
            os.remove(self.log_dir)

        Utils.check_directory(self.video_dir)
        Utils.rotate_directory(self.video_dir)

        logging.basicConfig(filename=self.log_dir, level=logging.DEBUG, format=self.FORMAT)
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
                        if count >= 1000:
                            Utils.rotate_directory(self.video_dir)
                            count = 0
                            time.sleep(120)

                if not saved:
                    logging.info("No data saved...waiting for 60 seconds")
                    time.sleep(60)
            except Exception, e:
                logging.info("Exception in wait loop " + str(e))
