#!/usr/bin/python

import psycopg2
import logging


class Data:
    def __init__(self, user, password, dbname):
        self.postgres = "dbname={0} user={1} password={2}".format(dbname, user, password)
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
            self.conn.reset()

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
            self.conn.reset()

    def get_links(self, username, filename):
        try:
            cur = self.conn.cursor()
            user_file = open(filename, 'w')
            query = "(select link from links where username = '{0}')".format(username)
            cur.copy_to(user_file, query, ",")
            user_file.close()
        except Exception, e:
            logging.warn("Exception get_links (insert) : " + str(e))
            self.conn.reset()

    def find_file(self, username, filename, partial=False):
        try:
            cur = self.conn.cursor()

            if partial:
                logging.info("---> looking for '%s' filename like '%s'", username, filename)
                cur.execute("select filename from links where username = %s and filename like %s", (username, filename))
            else:
                cur.execute("select filename from links where username = %s and filename = %s", (username, filename))

            return cur.fetchone()
        except Exception, e:
            logging.warn("Exception find_file (select) : " + str(e))
            self.conn.reset()
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
            self.conn.reset()

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
            self.conn.reset()