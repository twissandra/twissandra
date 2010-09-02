import threading
from cassandra.ttypes import (Clock, Column, ColumnOrSuperColumn, ConsistencyLevel,
                              Deletion, Mutation, SlicePredicate, SuperColumn)


class Mutator(object):
    """Batch update convenience mechanism.
       Queues insert/update/remove operations and executes them when the queue
       is filled up or explicitly using `send`.
    """

    def __init__(self, client, queue_size=100, write_consistency_level=None):
        self._buffer = []
        self._lock = threading.RLock()
        self.client = client
        self.limit = queue_size
        if write_consistency_level is None:
            self.write_consistency_level = ConsistencyLevel.ONE
        else:
            self.write_consistency_level = write_consistency_level

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.send()

    def _enqueue(self, key, column_family, mutations):
        self._lock.acquire()
        try:
            mutation = (key, column_family.column_family, mutations)
            self._buffer.append(mutation)
            if self.limit and len(self._buffer) >= self.limit:
                self.send()
        finally:
            self._lock.release()
        return self

    def send(self, write_consistency_level=None):
        if write_consistency_level is None:
            write_consistency_level = self.write_consistency_level
        mutations = {}
        self._lock.acquire()
        try:
            for key, column_family, cols in self._buffer:
                mutations.setdefault(key, {}).setdefault(column_family, []).extend(cols)
            if mutations:
                self.client.batch_mutate(mutations, write_consistency_level)
            self._buffer = []
        finally:
            self._lock.release()

    def _make_mutations_insert(self, column_family, columns, clock, ttl):
        _pack_name = column_family._pack_name
        _pack_value = column_family._pack_value
        for c, v in columns.iteritems():
            cos = ColumnOrSuperColumn()
            if column_family.super:
                subc = [Column(name=_pack_name(subname),
                               value=_pack_value(subvalue, subname),
                               clock=clock, ttl=ttl)
                            for subname, subvalue in v.iteritems()]
                cos.super_column = SuperColumn(name=_pack_name(c, True),
                                               columns=subc)
            else:
                cos.column = Column(name=_pack_name(c), value=_pack_value(v, c),
                                    clock=clock, ttl=ttl)
            yield Mutation(column_or_supercolumn=cos)

    def insert(self, column_family, key, columns, clock=None, ttl=None):
        if columns:
            clock = clock or Clock(timestamp=column_family.timestamp())
            mutations = self._make_mutations_insert(column_family, columns,
                                                    clock, ttl)
            self._enqueue(key, column_family, mutations)
        return self

    def remove(self, column_family, key, columns=None, super_column=None, clock=None):
        clock = clock or Clock(timestamp=column_family.timestamp())
        deletion = Deletion(clock=clock)
        if columns:
            _pack_name = column_family._pack_name
            packed_cols = [_pack_name(col, column_family.super)
                           for col in columns]
            deletion.predicate = SlicePredicate(column_names=packed_cols)
            if super_column:
                deletion.super_column = super_column
        mutation = Mutation(deletion=deletion)
        self._enqueue(key, column_family, (mutation,))
        return self


class CfMutator(Mutator):
    def __init__(self, column_family, queue_size=100, write_consistency_level=None):
        wcl = write_consistency_level or column_family.write_consistency_level
        super(CfMutator, self).__init__(column_family.client, queue_size=queue_size,
                                        write_consistency_level=wcl)
        self._column_family = column_family

    def insert(self, key, cols, clock=None, ttl=None):
        return super(CfMutator, self).insert(self._column_family, key, cols,
                                             clock=clock, ttl=ttl)

    def remove(self, key, columns=None, super_column=None, clock=None):
        return super(CfMutator, self).remove(self._column_family, key,
                                             columns=columns,
                                             super_column=super_column,
                                             clock=clock)

