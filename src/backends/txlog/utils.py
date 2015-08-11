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

"""Database access functions."""

from itertools import imap

from backends.filesync.data import dbmanager

from backends.txlog.model import TransactionLog


NEW_WORKER_LAST_ROW = (0, None)
CHUNK_SIZE = 10000
UNSEEN_EXPIRES = 24 * 60 * 60


def get_last_row(worker_name, store_name):
    """Try to get the id and timestamp of the last processed row for the
    specific worker name.

    If not found, get from the oldest possible row from the table.

    If still not found, return a default tuple for new workers.

    Use the store name, rather than a store reference, to sidestep potential
    thread safety problems.

    Transaction management should be performed by the caller.  Since this
    function is read-only, it may be called from code decorated with
    fsync_readonly, or as part of a block of operations decorated with
    fsync_commit.
    """
    worker_name = unicode(worker_name)
    store = dbmanager.get_shard_store(store_name)

    last_row = store.execute(u"""SELECT row_id, timestamp
        FROM txlog.db_worker_last_row
        WHERE worker_id=?""", (worker_name,)).get_one()
    if not last_row:
        last_row = store.execute(u"""SELECT row_id, timestamp
            FROM txlog.db_worker_last_row
            ORDER BY row_id LIMIT 1""").get_one()
    if not last_row:
        last_row = NEW_WORKER_LAST_ROW

    return last_row


def update_last_row(worker_name, row_id, timestamp, store_name):
    """Update the id and timestamp of the most recently processed transation
    log entry for a given worker.

    Use the store name, rather than a store reference, to sidestep potential
    thread safety problems.

    Transaction management should be performed by the caller.  Since this
    function writes to the database, it should be called from code blocks
    decorated with fsync_commit.
    """
    worker_name = unicode(worker_name)
    store = dbmanager.get_shard_store(store_name)
    result = store.execute(u"""UPDATE txlog.db_worker_last_row
        SET row_id=?, timestamp=?
        WHERE worker_id=?""", (row_id, timestamp, worker_name))
    if result.rowcount == 0:
        result = store.execute(
            u"""INSERT INTO txlog.db_worker_last_row
            (worker_id, row_id, timestamp) VALUES (?, ?, ?)""",
            (worker_name, row_id, timestamp))
        if result.rowcount == 0:
            raise RuntimeError(
                'Failed to update or insert last row id for worker %s' %
                worker_name)


def get_txn_recs(store_name, num_recs, last_id=0,
                 worker_id=None, expire_secs=None,
                 num_partitions=None, partition_id=None):
    """Attempt to read num_recs records from the transaction log, starting from
    the row after last_id, plus any records whose ID is in the db_worker_unseen
    table.

    Use the store name, rather than a store reference, to sidestep potential
    thread safety problems.

    Return a list of up to num_rec dicts representing records from the
    transaction log, starting from the row after last_id, or the beginning of
    the table if last_id is None.  If num_recs records are not available, all
    remaining records will be returned.  If no new records are available, an
    empty list is returned.

    Transaction management should be performed by the caller.  Since this
    function is read-only, it may be called from code decorated with
    fsync_readonly, or as part of a block of operations decorated with
    fsync_commit.
    """
    if expire_secs is None:
        expire_secs = UNSEEN_EXPIRES
    store = dbmanager.get_shard_store(store_name)
    parameters = (last_id, )
    select = u"""
        SELECT txlog.id, owner_id, node_id, volume_id, op_type, path,
               generation, timestamp, mimetype, old_path, extra_data
        FROM txlog.transaction_log AS txlog"""
    condition = u"WHERE id > ?"
    order_limit = u"ORDER BY id LIMIT {}".format(num_recs)

    if num_partitions is not None and partition_id is not None:
        unfilter_op_types = (
            TransactionLog.OP_SHARE_ACCEPTED,
            TransactionLog.OP_SHARE_DELETED,
        )
        unfilter_op_ids = (str(TransactionLog.OPERATIONS_MAP[id])
                           for id in unfilter_op_types)
        condition += u" AND (MOD(owner_id, ?) = ? OR op_type in ({}))".format(
            ','.join(unfilter_op_ids)
        )
        parameters = parameters + (num_partitions, partition_id)

    query = "({} {} {})".format(select, condition, order_limit)

    if worker_id is not None and expire_secs:
        worker_id = unicode(worker_id)
        join = (u"NATURAL JOIN txlog.db_worker_unseen as unseen "
                "WHERE unseen.worker_id = ? "
                "AND unseen.created > "
                "TIMEZONE('UTC'::text, NOW()) - INTERVAL '{} seconds'".format(
                    expire_secs))
        query += u" UNION ({} {} {}) {};".format(
            select, join, order_limit, order_limit
        )
        parameters = parameters + (worker_id,)

    records = store.execute(query, parameters)

    result = []
    for record in records:
        (row_id, owner_id, node_id, volume_id, op_type, path, generation,
         timestamp, mimetype, old_path, extra_data) = record
        op_type = TransactionLog.OPERATIONS_REVERSED_MAP[op_type]
        result.append(
            dict(txn_id=row_id, node_id=node_id, owner_id=owner_id,
                 volume_id=volume_id, op_type=op_type, path=path,
                 generation=generation, timestamp=timestamp,
                 mimetype=mimetype, old_path=old_path, extra_data=extra_data))

    # Now insert unseen directly into db_worker_unseen, avoiding duplicates by
    # joining again with db_worker_unseen.
    if result and result[-1]["txn_id"] > last_id and worker_id is not None:
        insert = (u"INSERT INTO txlog.db_worker_unseen (id, worker_id) "
                  "SELECT gs.id, ? FROM ("
                  "SELECT gs.id "
                  "FROM generate_series(?, ?) AS gs(id) "
                  "LEFT OUTER JOIN txlog.transaction_log AS txlog "
                  "ON gs.id = txlog.id "
                  "WHERE txlog.id IS NULL) AS gs "
                  "LEFT OUTER JOIN txlog.db_worker_unseen AS unseen "
                  "ON gs.id = unseen.id "
                  "AND unseen.worker_id = ? "
                  "WHERE unseen.id IS NULL;")
        store.execute(insert, (worker_id, last_id,
                               result[-1]["txn_id"], worker_id))

    return result


def ichunk(iter, chunk):
    i = 0
    while i < chunk:
        yield iter.next()
        i += 1


def delete_expired_unseen(store_name, worker_id, unseen_ids=None,
                          expire_secs=None):
    """Deletes expired unseen ids for a given worker id.

    If a list of unseen ids is given, also delete those explicitly.
    """
    if expire_secs is None:
        expire_secs = UNSEEN_EXPIRES
    worker_id = unicode(worker_id)
    store = dbmanager.get_shard_store(store_name)
    deleted = 0
    condition = (u"created < TIMEZONE('UTC'::text, NOW()) "
                 "     - INTERVAL '{} seconds'".format(expire_secs))
    query = (u"DELETE FROM txlog.db_worker_unseen "
             "WHERE worker_id = ? ")
    if unseen_ids is not None:
        fmt = u"({})".format
        if getattr(unseen_ids, "next", None) is None:
            unseen_ids = iter(unseen_ids)
        while True:
            unseen_args = u",".join(imap(fmt, ichunk(unseen_ids, CHUNK_SIZE)))
            if not unseen_args:
                break
            result = store.execute(
                query + u"AND ({} OR id = ANY(VALUES {}));".format(
                    condition, unseen_args), (worker_id,))
            deleted += result.rowcount
    else:
        query += u"AND {};".format(condition)
        result = store.execute(query, (worker_id,))
        deleted = result.rowcount
    return deleted


@dbmanager.fsync_commit
def delete_old_txlogs(store_name, timestamp_limit, quantity_limit=None):
    """Deletes the old transaction logs.

    Use the store name, rather than a store reference, to sidestep potential
    thread safety problems.

    Has to be given a datetime.datetime as a timestamp_limit; datetimes later
    than that will be filtered out, and won't be deleted.

    If quantity_limit is given, this will be the maximum number of entries to
    be deleted.
    """

    store = dbmanager.get_shard_store(store_name)
    parameters = [timestamp_limit]
    inner_select = "SELECT id FROM txlog.transaction_log WHERE timestamp <= ?"

    if quantity_limit is not None:
        inner_select += " LIMIT ?"
        parameters.append(quantity_limit)

    basic_query = ("DELETE FROM txlog.transaction_log WHERE id IN (%s);"
                   % inner_select)
    result = store.execute(basic_query, parameters)

    return result.rowcount


@dbmanager.fsync_commit
def delete_txlogs_slice(store_name, date, quantity_limit):
    """Deletes txlogs from a certain slice, by date and quantity limit.

    Almost the same as delete_old_txlogs, except that it deletes txlogs
    precisely from the provided date (a datetime.date object). Also, the
    quantity_limit parameter is mandatory."""

    store = dbmanager.get_shard_store(store_name)
    parameters = [date, quantity_limit]
    inner_select = ("SELECT id FROM txlog.transaction_log "
                    "WHERE timestamp::date = ? LIMIT ?")

    basic_query = ("DELETE FROM txlog.transaction_log WHERE id IN (%s);"
                   % inner_select)
    result = store.execute(basic_query, parameters)

    return result.rowcount


def get_row_by_time(store_name, timestamp):
    """Return the smaller txlog row id in that timestamp (or greater)."""
    store = dbmanager.get_shard_store(store_name)
    query = """
        SELECT id, timestamp FROM txlog.transaction_log
        WHERE timestamp >= ? ORDER BY id LIMIT 1;
    """
    result = store.execute(query, (timestamp,)).get_one()
    if result is None:
        txid, tstamp = None, None
    else:
        txid, tstamp = result
    return txid, tstamp


def keep_last_rows_for_worker_names(store_name, worker_names):
    """Clean rows from txlog.db_worker_last_row that don't match the given
    worker names."""
    store = dbmanager.get_shard_store(store_name)
    query = ("DELETE FROM txlog.db_worker_last_row "
             "WHERE worker_id NOT IN ?;")
    store.execute(query, (tuple(worker_names), ))
