#!/usr/bin/env python

# Copyright 2008-2015 Canonical
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# For further info, check  http://launchpad.net/filesync-server

"""Get stats for estimating keyword search on files."""

import os
import re
import unicodedata

from optparse import OptionParser

import psycopg2

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)


def get_keywords_from_path(volume_path):
    """Split keywords from a volume path."""
    # we do not index the root volume path
    clean_path = volume_path.replace("~/Ubuntu One", '')
    clean_path = unicodedata.normalize('NFKD', clean_path)
    clean_path = clean_path.encode('ASCII', 'ignore').lower()
    keywords = re.findall(r'\w+', clean_path)
    # convert to set for unique values
    return set(keywords)


SQL = """select v.owner_id, v.path volpath, o.path, o.name
         from object as o, userdefinedfolder v
         where o.volume_id=v.id and
               v.status='Live' and
               o.status='Live' and
               o.kind='File'
         order by v.owner_id, o.id
         OFFSET %d LIMIT %d"""


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("--host", dest="host")
    parser.add_option("--dbname", dest="dbname")
    parser.add_option("--user", dest="user")
    parser.add_option("--password", dest="password", default='')
    parser.add_option("--limit", dest="limit", type="int",
                      default=10000)
    (options, args) = parser.parse_args()

    conn_string = "host='%s' dbname='%s' user='%s'" % (
        options.host, options.dbname, options.user)
    if options.password:
        conn_string = conn_string + " password='%s'" % options.password

    last_user_id = 0
    offset = 0
    kw_cnt = 0
    node_cnt = 0
    kw_len = 0
    user_kw_cnt = 0
    user_cnt = 0
    volpath_len = 0

    cass_rows = 0
    cass_row_width_max = 0
    cass_col_bytes = 0

    conn = psycopg2.connect(conn_string)
    conn.set_session(readonly=True)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    curr = conn.cursor()
    try:
        while True:
            sql = SQL % (offset, options.limit)
            curr.execute(sql)
            rows = curr.fetchall()
            if not rows:
                break
            offset += options.limit
            for r in rows:
                node_cnt += 1
                user_id, volpath, path, name = r
                if user_id != last_user_id:
                    user_cnt += 1
                    user_kw_cnt = 0
                    last_user_id = user_id
                volpath = os.path.join(volpath, path, name)
                keywords = get_keywords_from_path(volpath)
                keywords_len = len(keywords)
                kw_cnt += keywords_len
                user_kw_cnt += keywords_len
                kw_bytes = sum(map(len, keywords))
                kw_len += kw_bytes
                volpath_len += len(volpath)
                # 42 is node_id (16 bytes) generation (8 bytes) + overhead
                cass_col_bytes += kw_bytes + (len(volpath) + 42) * keywords_len
                if user_kw_cnt > cass_row_width_max:
                        cass_row_width_max = user_kw_cnt
    finally:
        curr.close()
        conn.close()
    if node_cnt:
        print "Live Files:             % 15d" % node_cnt
        print "Keywords:               % 15d" % kw_cnt
        print "Avg Keyword per User:   % 15.2f" % (
            float(user_kw_cnt) / user_cnt)
        print "Avg Keyword Length:     % 15.2f" % (float(kw_len) / kw_cnt)
        print "Avg Volume Path Length: % 15.2f" % (
            float(volpath_len) / node_cnt)
        print "Cassandra Row Count:    % 15d" % user_cnt
        print "Cassandra Max Columns:  % 15d" % cass_row_width_max
        print "Cassandra Column bytes: % 15d" % cass_col_bytes
    else:
        print "No files found."
