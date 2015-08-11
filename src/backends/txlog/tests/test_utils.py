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

"""Tests for txlog utilities."""

import datetime

from mock import patch

from backends.filesync.data import dbmanager
from backends.filesync.data.testing.ormtestcase import ORMTestCase

from backends.txlog import utils
from backends.txlog.model import TransactionLog
from backends.txlog.tests.test_model import txlog_disabled


class TransactionLogUtilsTestCase(ORMTestCase):
    """Tests for the Materialised Views database access functions."""

    def setUp(self):
        super(TransactionLogUtilsTestCase, self).setUp()
        self._orig_make_user = self.obj_factory.make_user
        # Overwrite .obj_factory.make_user with a custom version that
        # doesn't create TransactionLogs as that would pollute our tests.
        p = patch.object(self.obj_factory, 'make_user')
        self.addCleanup(p.stop)
        mock_factory = p.start()
        mock_factory.side_effect = self._make_user_without_txlog
        self.store = dbmanager.get_shard_store(self.obj_factory.sstore_name)

    def _make_user_without_txlog(self, *args, **kwargs):
        """Custom make_user function that does not create TransactionLogs."""
        with txlog_disabled():
            return self._orig_make_user(*args, **kwargs)

    def _create_db_worker_last_row_entry(self, worker_name, txlog):
        """Create a new entry on the txlog.db_worker_last_row table."""
        worker_name = unicode(worker_name)
        self.obj_factory.sstore.execute(
            u"""INSERT INTO txlog.db_worker_last_row
            (worker_id, row_id, timestamp) VALUES (?, ?, ?)""",
            params=(worker_name, txlog.id, txlog.timestamp))

    def _find_last_row_worker_names(self):
        """Find all worker names from the db_worker_last_row table."""
        result = self.obj_factory.sstore.execute(
            u"""SELECT worker_id
            FROM txlog.db_worker_last_row""")
        return [row[0] for row in result]

    def test_get_last_row_with_no_data(self):
        """Test the get_last_row function when no data is present."""
        # First, check the db directly to ensure it is empty.
        result = self.obj_factory.sstore.execute(
            u"""SELECT row_id
            FROM txlog.db_worker_last_row""")
        self.assertEqual(0, result.rowcount)
        self.assertEqual(utils.NEW_WORKER_LAST_ROW, utils.get_last_row(
            'some worker', self.obj_factory.sstore_name))

    def test_get_last_row_with_other_data_returns_the_oldest_one(self):
        """get_last_row returns the row for the oldest txlog ID in the
        table, if the worker name is not found for that."""
        txlog1 = self.obj_factory.make_transaction_log()
        txlog2 = self.obj_factory.make_transaction_log()
        txlog3 = self.obj_factory.make_transaction_log()

        self._create_db_worker_last_row_entry(
            self.obj_factory.get_unique_unicode(), txlog3)
        self._create_db_worker_last_row_entry(
            self.obj_factory.get_unique_unicode(), txlog1)
        self._create_db_worker_last_row_entry(
            self.obj_factory.get_unique_unicode(), txlog2)

        self.assertEqual((txlog1.id, txlog1.timestamp), utils.get_last_row(
            'some worker', self.obj_factory.sstore_name))

    def test_get_last_row_with_same_data_returns_the_exact_one(self):
        """Test that get_last_row returns the row for the exact txlog ID in the
        table, if the worker name is found for that."""
        txlog1 = self.obj_factory.make_transaction_log()
        txlog2 = self.obj_factory.make_transaction_log()
        txlog3 = self.obj_factory.make_transaction_log()

        worker_name = self.obj_factory.get_unique_unicode()
        self._create_db_worker_last_row_entry(worker_name, txlog3)
        self._create_db_worker_last_row_entry(
            self.obj_factory.get_unique_unicode(), txlog1)
        self._create_db_worker_last_row_entry(
            self.obj_factory.get_unique_unicode(), txlog2)

        self.assertEqual((txlog3.id, txlog3.timestamp), utils.get_last_row(
            worker_name, self.obj_factory.sstore_name))

    def test_update_last_row_with_no_data(self):
        """Test the update_last_row function when no data is present."""
        txlog = self.obj_factory.make_transaction_log()
        worker_name = self.obj_factory.get_unique_unicode()
        utils.update_last_row(
            worker_name=worker_name, row_id=txlog.id,
            timestamp=txlog.timestamp, store_name=self.obj_factory.sstore_name)
        result = self.obj_factory.sstore.execute(
            u"""SELECT row_id, timestamp
            FROM txlog.db_worker_last_row
            WHERE worker_id=?""", (worker_name,)).get_one()
        self.assertEqual((txlog.id, txlog.timestamp), result)

    def test_update_last_row_with_data(self):
        """Test the update_last_row function when data for this worker is
        present.
        """
        txlog = self.obj_factory.make_transaction_log()
        txlog2 = self.obj_factory.make_transaction_log()
        worker_name = self.obj_factory.get_unique_unicode()
        self._create_db_worker_last_row_entry(worker_name, txlog)
        utils.update_last_row(
            worker_name=worker_name, row_id=txlog2.id,
            timestamp=txlog2.timestamp,
            store_name=self.obj_factory.sstore_name)
        result = self.obj_factory.sstore.execute(
            u"""SELECT row_id, timestamp
            FROM txlog.db_worker_last_row
            WHERE worker_id=?""", (worker_name,)).get_one()
        self.assertEqual((txlog2.id, txlog2.timestamp), result)

    def test_update_last_row_failure(self):
        """Test that an exception is raised if update_last_row fails to
        either update an existing row or insert a new one.
        """

        class DummyStore(object):
            """A dummy store that returns results with a rowcount of 0."""

            def execute(self, statement, params=None, noresult=False):
                """Dummy execute method that always returns a Result object
                whose rowcount property is 0.
                """
                return type('DummyResultSet', (object,), dict(rowcount=0))

        with patch.object(dbmanager, 'get_shard_store') as mock_get_shard:
            mock_get_shard.return_value = DummyStore()

            self.assertRaises(
                RuntimeError, utils.update_last_row,
                worker_name=u'test_worker_name', row_id=1,
                timestamp=datetime.datetime.utcnow(),
                store_name=self.obj_factory.sstore_name)

    def _convert_txlogs_to_dicts(self, txlogs):
        """Convert a list of TransactionLog objects into dictionaries.

        These dictionaries have the same keys as the ones in the dictionaries
        returned by get_txn_recs.
        """
        dicts = []
        for txlog in txlogs:
            dicts.append(dict(
                txn_id=txlog.id, node_id=str(txlog.node_id),
                owner_id=txlog.owner_id, volume_id=str(txlog.volume_id),
                op_type=txlog.op_type, path=txlog.path,
                generation=txlog.generation, timestamp=txlog.timestamp,
                mimetype=txlog.mimetype, old_path=txlog.old_path,
                extra_data=txlog.extra_data))
        return dicts

    def test_get_txn_recs_no_previous_no_txns(self):
        """Test getting a batch of transactions when we have not previously
        processed any rows and the transaction_log table is empty.
        """
        txlist = utils.get_txn_recs(
            self.obj_factory.sstore_name, num_recs=5, last_id=0)
        self.assertEqual([], txlist)

    def test_get_txn_recs_no_previous_small_result_set(self):
        """Test getting a batch of transactions when we have not previously
        processed any rows and the number of rows in the transaction_log table
        is smaller than the number requested.
        """
        txlogs = [self.obj_factory.make_transaction_log(),
                  self.obj_factory.make_transaction_log()]
        txlist = utils.get_txn_recs(
            self.obj_factory.sstore_name, num_recs=5, last_id=0)
        self.assertEqual(self._convert_txlogs_to_dicts(txlogs), txlist)

    def test_get_txn_recs_no_previous_exact_result_set(self):
        """Test getting a batch of transactions when we have not previously
        processed any rows and the number of rows in the transaction_log table
        is exactly the number requested.
        """
        txlogs = [self.obj_factory.make_transaction_log(),
                  self.obj_factory.make_transaction_log()]
        txlist = utils.get_txn_recs(
            self.obj_factory.sstore_name, num_recs=2, last_id=0)
        self.assertEqual(self._convert_txlogs_to_dicts(txlogs), txlist)

    def test_get_txn_recs_no_previous_large_result_set(self):
        """Test getting a batch of transactions when we have not previously
        processed any rows and the number of rows in the transaction_log table
        is larger than the number requested.
        """
        txlogs = [self.obj_factory.make_transaction_log(),
                  self.obj_factory.make_transaction_log()]
        txlist = utils.get_txn_recs(
            self.obj_factory.sstore_name, num_recs=1, last_id=0)
        self.assertEqual(self._convert_txlogs_to_dicts(txlogs[:1]), txlist)

    def test_get_txn_recs_previous_no_new(self):
        """Test getting a batch of transactions when we have previously
        processed rows and there are no newer rows in the transaction_log
        table.
        """
        self.obj_factory.make_transaction_log()
        self.obj_factory.make_transaction_log()
        txlist = utils.get_txn_recs(
            self.obj_factory.sstore_name, num_recs=1, last_id=2)
        self.assertEqual([], txlist)

    def test_get_txn_recs_previous_small_new(self):
        """Test getting a batch of transactions when we have previously
        processed rows and there are fewer newer rows in the transaction_log
        table than we requested.
        """
        txlogs = [self.obj_factory.make_transaction_log(),
                  self.obj_factory.make_transaction_log()]
        txlist = utils.get_txn_recs(
            self.obj_factory.sstore_name, num_recs=5, last_id=1)
        self.assertEqual(self._convert_txlogs_to_dicts(txlogs[1:]), txlist)

    def test_get_txn_recs_previous_exact_new(self):
        """Test getting a batch of transactions when we have previously
        processed rows and there are the exact number of newer rows in the
        transaction_log table that we requested.
        """
        txlogs = [self.obj_factory.make_transaction_log(),
                  self.obj_factory.make_transaction_log()]
        txlist = utils.get_txn_recs(
            self.obj_factory.sstore_name, num_recs=1, last_id=1)
        self.assertEqual(self._convert_txlogs_to_dicts(txlogs[1:]), txlist)

    def test_get_txn_recs_previous_large_new(self):
        """Test getting a batch of transactions when we have previously
        processed rows and there are the more newer rows in the
        transaction_log table than we requested.
        """
        txlogs = [self.obj_factory.make_transaction_log(),
                  self.obj_factory.make_transaction_log(),
                  self.obj_factory.make_transaction_log()]
        txlist = utils.get_txn_recs(
            self.obj_factory.sstore_name, num_recs=1, last_id=1)
        self.assertEqual(self._convert_txlogs_to_dicts(txlogs[1:2]), txlist)

    def test_get_txn_recs_respects_order(self):
        """Test that transaction log entries are returned in order."""
        txlogs = [self.obj_factory.make_transaction_log(tx_id=3),
                  self.obj_factory.make_transaction_log(tx_id=2),
                  self.obj_factory.make_transaction_log(tx_id=1)]
        txlist = utils.get_txn_recs(
            self.obj_factory.sstore_name, num_recs=3, last_id=0)
        txlogs.sort(key=lambda x: x.id)
        self.assertEqual(self._convert_txlogs_to_dicts(txlogs), txlist)

    def test_get_txn_recs_unseen(self):
        """Test getting a batch of transactions when there are unseen ids
        records those as unseen. Querying again returns unseen transactions if
        they are now present.
        """
        txlogs = [self.obj_factory.make_transaction_log(tx_id=1),
                  self.obj_factory.make_transaction_log(tx_id=3)]
        worker_id = self.obj_factory.get_unique_unicode()
        txlist = utils.get_txn_recs(
            self.obj_factory.sstore_name, num_recs=3, worker_id=worker_id)
        self.assertEqual(
            self._convert_txlogs_to_dicts([txlogs[0], txlogs[1]]), txlist)
        unseen = self.obj_factory.make_transaction_log(tx_id=2)
        txlist = utils.get_txn_recs(
            self.obj_factory.sstore_name, num_recs=3,
            last_id=txlogs[1].id, worker_id=worker_id)
        self.assertEqual(
            self._convert_txlogs_to_dicts([unseen]), txlist)

    def test_get_txn_recs_retry_list_no_new_or_retry(self):
        """Test getting a batch of transactions when there are unseen ids
        records those as unseen. Querying again when unseen isn't available yet
        returns nothing.
        """
        txlogs = [self.obj_factory.make_transaction_log(tx_id=1),
                  self.obj_factory.make_transaction_log(tx_id=3)]
        worker_id = self.obj_factory.get_unique_unicode()
        txlist = utils.get_txn_recs(
            self.obj_factory.sstore_name, num_recs=3, worker_id=worker_id)
        self.assertEqual(
            self._convert_txlogs_to_dicts([txlogs[0], txlogs[1]]), txlist)
        txlist = utils.get_txn_recs(
            self.obj_factory.sstore_name, num_recs=3,
            last_id=txlogs[1].id, worker_id=worker_id)
        self.assertEqual(list(), txlist)

    def test_get_txn_recs_for_partition(self):
        """Get txlogs for the provided partition ID.

        When owner_id % num_partitions == partition_id, the txlog is added to
        the result set, so that it matches the filter by partition. Also, any
        txlog that is related to sharing is also returned, no matter what the
        owner_id is.
        """
        owner_id = 1
        num_partitions = 8
        partition_id = owner_id % num_partitions

        txlogs = [
            self.obj_factory.make_transaction_log(),
            self.obj_factory.make_transaction_log(owner_id=2),  # Different one
            self.obj_factory.make_transaction_log(),
            # Share txlogs, but with a different owner, are also returned.
            self.obj_factory.make_transaction_log(
                owner_id=2, op_type=TransactionLog.OP_SHARE_ACCEPTED),
            self.obj_factory.make_transaction_log(
                owner_id=2, op_type=TransactionLog.OP_SHARE_DELETED),
        ]
        expected_txlogs = [txlogs[0], txlogs[2], txlogs[3], txlogs[4]]
        txlist = utils.get_txn_recs(
            self.obj_factory.sstore_name, num_recs=5, last_id=0,
            num_partitions=num_partitions, partition_id=partition_id
        )
        self.assertEqual(self._convert_txlogs_to_dicts(expected_txlogs),
                         txlist)

    def test_maintains_newish_txlogs_when_purging(self):
        """Test that txnlogs not old enough are maintained, instead of being
        deleted."""

        now = datetime.datetime.utcnow()
        limit_datetime = now - datetime.timedelta(days=7)
        # Not so old
        old_datetime = limit_datetime + datetime.timedelta(seconds=1)

        self.obj_factory.make_transaction_log(tx_id=1)
        self.obj_factory.make_transaction_log(tx_id=2, timestamp=old_datetime)
        self.obj_factory.make_transaction_log(tx_id=3)
        self.obj_factory.make_transaction_log(tx_id=4, timestamp=old_datetime)
        self.store.commit()

        removed = utils.delete_old_txlogs(self.obj_factory.sstore_name,
                                          timestamp_limit=limit_datetime)

        self.store.rollback()  # Shouldn't affect the deletion result

        txlist = utils.get_txn_recs(
            self.obj_factory.sstore_name, num_recs=4, last_id=0)
        self.assertEqual(len(txlist), 4)
        self.assertEqual(removed, 0)
        ids = sorted(int(txdict['txn_id']) for txdict in txlist)
        self.assertEqual(ids, [1, 2, 3, 4])

    def test_deletes_old_enough_txlogs(self):
        """Test that txnlogs old enough are deleted."""

        now = datetime.datetime.utcnow()
        timestamp_limit = now - datetime.timedelta(days=7)
        # Old enough
        old_datetime = timestamp_limit

        txlogs = [
            self.obj_factory.make_transaction_log(tx_id=1),
            self.obj_factory.make_transaction_log(
                tx_id=2, timestamp=old_datetime),
            self.obj_factory.make_transaction_log(tx_id=3),
            self.obj_factory.make_transaction_log(
                tx_id=4, timestamp=old_datetime),
        ]
        self.store.commit()

        removed = utils.delete_old_txlogs(self.obj_factory.sstore_name,
                                          timestamp_limit=timestamp_limit)

        self.store.rollback()  # Shouldn't affect the deletion result

        txlist = utils.get_txn_recs(
            self.obj_factory.sstore_name, num_recs=len(txlogs), last_id=0)
        self.assertEqual(len(txlist), 2)
        self.assertEqual(removed, 2)
        ids = sorted(int(txdict['txn_id']) for txdict in txlist)
        self.assertEqual(ids, [1, 3])

    def test_deletes_old_txlogs_within_quantity_limit(self):
        """Test that txnlogs old enough are deleted and are within the quantity
        limit given."""

        now = datetime.datetime.utcnow()
        timestamp_limit = now - datetime.timedelta(days=7)
        # Old enough
        old_datetime = timestamp_limit
        quantity_limit = 2

        txlogs = [
            self.obj_factory.make_transaction_log(tx_id=1),
            self.obj_factory.make_transaction_log(
                tx_id=2, timestamp=old_datetime),
            self.obj_factory.make_transaction_log(tx_id=3),
            self.obj_factory.make_transaction_log(
                tx_id=4, timestamp=old_datetime),
            self.obj_factory.make_transaction_log(
                tx_id=5, timestamp=old_datetime),
        ]
        self.store.commit()

        removed = utils.delete_old_txlogs(self.obj_factory.sstore_name,
                                          timestamp_limit=timestamp_limit,
                                          quantity_limit=quantity_limit)

        self.store.rollback()  # Shouldn't affect the deletion result

        txlist = utils.get_txn_recs(
            self.obj_factory.sstore_name, num_recs=len(txlogs), last_id=0)
        self.assertEqual(len(txlist), 3)
        self.assertEqual(removed, quantity_limit)
        ids = sorted(int(txdict['txn_id']) for txdict in txlist)
        self.assertEqual(ids, [1, 3, 5])

    def test_deletes_txlogs_slice(self):
        """Delete a txlog slice by date and quantity."""

        now = datetime.datetime.utcnow()
        timestamp_limit = now - datetime.timedelta(days=7)
        # Old enough
        old_dt = timestamp_limit
        quantity_limit = 2

        txlogs = [
            self.obj_factory.make_transaction_log(tx_id=1),
            self.obj_factory.make_transaction_log(tx_id=2, timestamp=old_dt),
            self.obj_factory.make_transaction_log(tx_id=3),
            self.obj_factory.make_transaction_log(tx_id=4, timestamp=old_dt),
            self.obj_factory.make_transaction_log(tx_id=5),
        ]
        self.store.commit()

        removed = utils.delete_txlogs_slice(self.obj_factory.sstore_name,
                                            date=now.date(),
                                            quantity_limit=quantity_limit)

        self.store.rollback()  # Shouldn't affect the deletion result

        txlist = utils.get_txn_recs(
            self.obj_factory.sstore_name, num_recs=len(txlogs), last_id=0)
        self.assertEqual(len(txlist), 3)
        self.assertEqual(removed, quantity_limit)
        ids = sorted(int(txdict['txn_id']) for txdict in txlist)
        self.assertIn(2, ids)
        self.assertIn(4, ids)

    def test_get_row_by_time_with_no_data(self):
        """Test the get_row_by_time function when no data is present."""
        txid, _ = utils.get_row_by_time(self.obj_factory.sstore_name,
                                        datetime.datetime.utcnow())
        self.assertEqual(txid, None)

    def test_get_row_by_time_with_data(self):
        """Test get_row_by_time function when data is present."""
        ts = datetime.datetime.utcnow()
        txlogs = [
            self.obj_factory.make_transaction_log(
                timestamp=ts + datetime.timedelta(i, 0)) for i in range(5)]
        tstamp = txlogs[2].timestamp
        txid, newtstamp = utils.get_row_by_time(self.obj_factory.sstore_name,
                                                tstamp)
        self.assertEqual(txid, txlogs[2].id)
        self.assertEqual(newtstamp, tstamp)

    def test_get_row_by_time_timestamp_twice(self):
        """Test get_row_by_time having two lines with same timestamp."""
        ts = datetime.datetime.utcnow()
        txlogs = [
            self.obj_factory.make_transaction_log(
                timestamp=ts + datetime.timedelta(i, 0)) for i in range(5)]
        # put the timestamp of [3] into [1], the function should return the
        # id of [1]
        tstamp = txlogs[1].timestamp = txlogs[3].timestamp

        txid, newtstamp = utils.get_row_by_time(self.obj_factory.sstore_name,
                                                tstamp)
        self.assertEqual(txid, txlogs[1].id)
        self.assertEqual(newtstamp, tstamp)

    def test_get_row_by_time_not_exact(self):
        """Test get_row_by_time not giving an exact timestamp."""
        ts = datetime.datetime.utcnow()
        txlogs = [
            self.obj_factory.make_transaction_log(
                timestamp=ts + datetime.timedelta(i, 0)) for i in range(5)]

        # get a timestamp in the middle of [2] and [3], the function should
        # return the id of [3]
        tx2, tx3 = txlogs[2:4]
        delta = (txlogs[3].timestamp - txlogs[2].timestamp) / 2
        tstamp = txlogs[2].timestamp + delta

        txid, newtstamp = utils.get_row_by_time(self.obj_factory.sstore_name,
                                                tstamp)
        self.assertEqual(txid, txlogs[3].id)
        self.assertEqual(newtstamp, txlogs[3].timestamp)

    def test_get_row_by_time_nothing_found(self):
        """Test get_row_by_time with a big enough timestamp."""
        txlogs = [self.obj_factory.make_transaction_log() for i in range(2)]
        tstamp = txlogs[-1].timestamp + datetime.timedelta(seconds=1)
        txid, newtstamp = utils.get_row_by_time(self.obj_factory.sstore_name,
                                                tstamp)
        self.assertEqual(txid, None)
        self.assertEqual(newtstamp, None)

    def test_cleans_last_rows_for_workers_not_in_list(self):
        """Test that keep_last_rows_for_worker_names removes all rows from
        workers not in the list of given names."""
        initial_workers = [
            u'worker1',
            u'worker2',
            u'worker3',
            u'worker4',
        ]
        kept_workers = [
            u'worker1',
            u'worker2',
            u'worker4',
        ]
        for worker_name in initial_workers:
            txlog = self.obj_factory.make_transaction_log()
            self._create_db_worker_last_row_entry(worker_name, txlog)

        utils.keep_last_rows_for_worker_names(self.obj_factory.sstore_name,
                                              kept_workers)

        actual_worker_names = self._find_last_row_worker_names()
        self.assertEqual(actual_worker_names, kept_workers)

    def test_cleans_last_rows_for_workers_not_in_list_of_strings(self):
        """Test that keep_last_rows_for_worker_names removes all rows from
        workers not in the list of given names as plain strings."""
        initial_workers = [
            'worker1',
            'worker2',
            'worker3',
            'worker4',
        ]
        kept_workers = [
            'worker1',
            'worker2',
            'worker4',
        ]
        for worker_name in initial_workers:
            txlog = self.obj_factory.make_transaction_log()
            self._create_db_worker_last_row_entry(worker_name, txlog)

        utils.keep_last_rows_for_worker_names(self.obj_factory.sstore_name,
                                              kept_workers)

        actual_worker_names = self._find_last_row_worker_names()
        self.assertEqual(actual_worker_names, kept_workers)
