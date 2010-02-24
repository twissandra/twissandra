from cassandra.ttypes import Column, ColumnOrSuperColumn, ColumnParent, \
    ColumnPath, ConsistencyLevel, NotFoundException, SlicePredicate, \
    SliceRange, SuperColumn

import time

__all__ = ['gm_timestamp', 'ColumnFamily']

def gm_timestamp():
    """
    Returns
    -------
    int : UNIX epoch time in GMT
    """
    return int(time.mktime(time.gmtime()))

def create_SlicePredicate(columns, column_start, column_finish, column_reversed, column_count):
    if columns is not None:
        return SlicePredicate(column_names=columns)
    sr = SliceRange(start=column_start, finish=column_finish,
                    reversed=column_reversed, count=column_count)
    return SlicePredicate(slice_range=sr)

class ColumnFamily(object):
    def __init__(self, client, keyspace, column_family,
                 buffer_size=1024,
                 read_consistency_level=ConsistencyLevel.ONE,
                 write_consistency_level=ConsistencyLevel.ZERO,
                 timestamp=gm_timestamp, super=False,
                 dict_class=dict):
        """
        Construct a ColumnFamily

        Parameters
        ----------
        client   : cassandra.Cassandra.Client
            Cassandra client with thrift API
        keyspace : str
            The Keyspace this ColumnFamily belongs to
        column_family : str
            The name of this ColumnFamily
        buffer_size : int
            When calling get_range(), the intermediate results need to be
            buffered if we are fetching many rows, otherwise the Cassandra
            server will overallocate memory and fail.  This is the size of
            that buffer.
        read_consistency_level : ConsistencyLevel
            Affects the guaranteed replication factor before returning from
            any read operation
        write_consistency_level : ConsistencyLevel
            Affects the guaranteed replication factor before returning from
            any write operation
        timestamp : function
            The default timestamp function returns:
            int(time.mktime(time.gmtime()))
            Or the number of seconds since Unix epoch in GMT.
            Set timestamp to replace the default timestamp function with your
            own.
        super : bool
            Whether this ColumnFamily has SuperColumns
        dict_class : class (must act like the dict type)
            The default dict_class is dict.
            If the order of columns matter to you, pass your own dictionary
            class, or python 2.7's new collections.OrderedDict. All returned
            rows and subcolumns are instances of this.
        """
        self.client = client
        self.keyspace = keyspace
        self.column_family = column_family
        self.buffer_size = buffer_size
        self.read_consistency_level = read_consistency_level
        self.write_consistency_level = write_consistency_level
        self.timestamp = timestamp
        self.super = super
        self.dict_class = dict_class

    def _convert_Column_to_base(self, column, include_timestamp):
        if include_timestamp:
            return (column.value, column.timestamp)
        return column.value

    def _convert_SuperColumn_to_base(self, super_column, include_timestamp):
        ret = self.dict_class()
        for column in super_column.columns:
            ret[column.name] = self._convert_Column_to_base(column, include_timestamp)
        return ret

    def _convert_ColumnOrSuperColumns_to_dict_class(self, list_col_or_super, include_timestamp):
        ret = self.dict_class()
        for col_or_super in list_col_or_super:
            if col_or_super.super_column is not None:
                col = col_or_super.super_column
                ret[col.name] = self._convert_SuperColumn_to_base(col, include_timestamp)
            else:
                col = col_or_super.column
                ret[col.name] = self._convert_Column_to_base(col, include_timestamp)
        return ret

    def get(self, key, columns=None, column_start="", column_finish="",
            column_reversed=False, column_count=100, include_timestamp=False,
            super_column=None):
        """
        Fetch a key from a Cassandra server
        
        Parameters
        ----------
        key : str
            The key to fetch
        columns : [str]
            Limit the columns fetched to the specified list
        column_start : str
            Only fetch when a column is >= column_start
        column_finish : str
            Only fetch when a column is <= column_finish
        column_reversed : bool
            Fetch the columns in reverse order. This will do nothing unless
            you passed a dict_class to the constructor.
        column_count : int
            Limit the number of columns fetched per key
        include_timestamp : bool
            If true, return a (value, timestamp) tuple for each column
        super_column : str
            Return columns only in this super_column

        Returns
        -------
        if include_timestamp == True: {'column': ('value', timestamp)}
        else: {'column': 'value'}
        """
        cp = ColumnParent(column_family=self.column_family, super_column=super_column)
        sp = create_SlicePredicate(columns, column_start, column_finish,
                                   column_reversed, column_count)

        list_col_or_super = self.client.get_slice(self.keyspace, key, cp, sp,
                                                  self.read_consistency_level)
        if len(list_col_or_super) == 0:
            raise NotFoundException()
        return self._convert_ColumnOrSuperColumns_to_dict_class(list_col_or_super, include_timestamp)

    def multiget(self, keys, columns=None, column_start="", column_finish="",
                 column_reversed=False, column_count=100, include_timestamp=False,
                 super_column=None):
        """
        Fetch multiple key from a Cassandra server
        
        Parameters
        ----------
        keys : [str]
            A list of keys to fetch
        columns : [str]
            Limit the columns fetched to the specified list
        column_start : str
            Only fetch when a column is >= column_start
        column_finish : str
            Only fetch when a column is <= column_finish
        column_reversed : bool
            Fetch the columns in reverse order. This will do nothing unless
            you passed a dict_class to the constructor.
        column_count : int
            Limit the number of columns fetched per key
        include_timestamp : bool
            If true, return a (value, timestamp) tuple for each column
        super_column : str
            Return columns only in this super_column

        Returns
        -------
        if include_timestamp == True: {'key': {'column': ('value', timestamp)}}
        else: {'key': {'column': 'value'}}
        """
        cp = ColumnParent(column_family=self.column_family, super_column=super_column)
        sp = create_SlicePredicate(columns, column_start, column_finish,
                                   column_reversed, column_count)

        keymap = self.client.multiget_slice(self.keyspace, keys, cp, sp,
                                            self.read_consistency_level)

        ret = dict()
        for key, columns in keymap.iteritems():
            if len(columns) > 0:
                ret[key] = self._convert_ColumnOrSuperColumns_to_dict_class(columns, include_timestamp)
        return ret

    def get_count(self, key, super_column=None):
        """
        Count the number of columns for a key

        Parameters
        ----------
        key : str
            The key with which to count columns
        super_column : str
            Count the columns only in this super_column

        Returns
        -------
        int Count of columns
        """
        cp = ColumnParent(column_family=self.column_family, super_column=super_column)
        return self.client.get_count(self.keyspace, key, cp,
                                     self.read_consistency_level)

    def get_range(self, start="", finish="", columns=None, column_start="",
                  column_finish="", column_reversed=False, column_count=100,
                  row_count=None, include_timestamp=False,
                  super_column=None):
        """
        Get an iterator over keys in a specified range
        
        Parameters
        ----------
        start : str
            Start from this key (inclusive)
        finish : str
            End at this key (inclusive)
        columns : [str]
            Limit the columns fetched to the specified list
        column_start : str
            Only fetch when a column is >= column_start
        column_finish : str
            Only fetch when a column is <= column_finish
        column_reversed : bool
            Fetch the columns in reverse order. This will do nothing unless
            you passed a dict_class to the constructor.
        column_count : int
            Limit the number of columns fetched per key
        row_count : int
            Limit the number of rows fetched
        include_timestamp : bool
            If true, return a (value, timestamp) tuple for each column
        super_column : string
            Return columns only in this super_column

        Returns
        -------
        iterator over ('key', {'column': 'value'})
        """
        cp = ColumnParent(column_family=self.column_family, super_column=super_column)
        sp = create_SlicePredicate(columns, column_start, column_finish,
                                   column_reversed, column_count)

        count = 0
        i = 0
        last_key = start
        
        buffer_size = self.buffer_size
        if row_count is not None:
            buffer_size = min(row_count, self.buffer_size)
        while True:
            key_slices = self.client.get_range_slice(self.keyspace, cp, sp, last_key,
                                                     finish, buffer_size,
                                                     self.read_consistency_level)
            # This may happen if nothing was ever inserted
            if key_slices is None:
                return
            for j, key_slice in enumerate(key_slices):
                # Ignore the first element after the first iteration
                # because it will be a duplicate.
                if j == 0 and i != 0:
                    continue
                yield (key_slice.key,
                       self._convert_ColumnOrSuperColumns_to_dict_class(key_slice.columns, include_timestamp))
                count += 1
                if row_count is not None and count >= row_count:
                    return

            if len(key_slices) != self.buffer_size:
                return
            last_key = key_slices[-1].key
            i += 1

    def insert(self, key, columns):
        """
        Insert or update columns for a key

        Parameters
        ----------
        key : str
            The key to insert or update the columns at
        columns : dict
            Column: {'column': 'value'}
            SuperColumn: {'column': {'subcolumn': 'value'}}
            The columns or supercolumns to insert or update

        Returns
        -------
        int timestamp
        """
        timestamp = self.timestamp()

        cols = []
        for c, v in columns.iteritems():
            if self.super:
                subc = [Column(name=subname, value=subvalue, timestamp=timestamp) \
                        for subname, subvalue in v.iteritems()]
                column = SuperColumn(name=c, columns=subc)
                cols.append(ColumnOrSuperColumn(super_column=column))
            else:
                column = Column(name=c, value=v, timestamp=timestamp)
                cols.append(ColumnOrSuperColumn(column=column))
        self.client.batch_insert(self.keyspace, key,
                                 {self.column_family: cols},
                                 self.write_consistency_level)
        return timestamp

    def remove(self, key, column=None):
        """
        Remove a specified key or column

        Parameters
        ----------
        key : str
            The key to remove. If column is not set, remove all columns
        column : str
            If set, remove only this column or supercolumn

        Returns
        -------
        int timestamp
        """
        if self.super:
            cp = ColumnPath(column_family=self.column_family, super_column=column)
        else:
            cp = ColumnPath(column_family=self.column_family, column=column)
        timestamp = self.timestamp()
        self.client.remove(self.keyspace, key, cp, timestamp,
                           self.write_consistency_level)
        return timestamp
