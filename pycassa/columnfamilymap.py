from pycassa.types import Column

__all__ = ['ColumnFamilyMap']

def create_instance(cls, **kwargs):
    instance = cls()
    instance.__dict__.update(kwargs)
    return instance

def combine_columns(column_dict, columns):
    combined_columns = {}
    for column, type in column_dict.iteritems():
        combined_columns[column] = type.default
    for column, value in columns.iteritems():
        combined_columns[column] = column_dict[column].unpack(value)
    return combined_columns

class ColumnFamilyMap(object):
    def __init__(self, cls, column_family, columns=None):
        """
        Construct a ObjectFamily

        Parameters
        ----------
        cls      : class
            Instances of cls are generated on get*() requests
        column_family: ColumnFamily
            The ColumnFamily to tie with cls
        """
        self.cls = cls
        self.column_family = column_family

        self.columns = {}
        for name, column in self.cls.__dict__.iteritems():
            if not isinstance(column, Column):
                continue

            self.columns[name] = column

    def get(self, key, *args, **kwargs):
        """
        Fetch a key from a Cassandra server
        
        Parameters
        ----------
        key : str
            The key to fetch
        columns : [str]
            Limit the columns fetched to the specified list
        column_start = str
            Only fetch when a column is >= column_start
        column_finish = str
            Only fetch when a column is <= column_finish
        column_reversed = bool
            Fetch the columns in reverse order. Currently this does nothing
            because columns are converted into a dict.
        column_count = int
            Limit the number of columns fetched per key
        include_timestamp = bool
            If true, return a (value, timestamp) tuple for each column

        Returns
        -------
        Class instance
        """
        if 'columns' not in kwargs:
            kwargs['columns'] = self.columns.keys()
        columns = self.column_family.get(key, *args, **kwargs)
        columns = combine_columns(self.columns, columns)
        return create_instance(self.cls, key=key, **columns)

    def multiget(self, *args, **kwargs):
        """
        Fetch multiple key from a Cassandra server
        
        Parameters
        ----------
        keys : [str]
            A list of keys to fetch
        columns : [str]
            Limit the columns fetched to the specified list
        column_start = str
            Only fetch when a column is >= column_start
        column_finish = str
            Only fetch when a column is <= column_finish
        column_reversed = bool
            Fetch the columns in reverse order. Currently this does nothing
            because columns are converted into a dict.
        column_count = int
            Limit the number of columns fetched per key
        include_timestamp = bool
            If true, return a (value, timestamp) tuple for each column

        Returns
        -------
        {'key': Class instance} 
        """
        if 'columns' not in kwargs:
            kwargs['columns'] = self.columns.keys()
        kcmap = self.column_family.multiget(*args, **kwargs)
        ret = {}
        for key, columns in kcmap.iteritems():
            columns = combine_columns(self.columns, columns)
            ret[key] = create_instance(self.cls, key=key, **columns)
        return ret

    def get_count(self, *args, **kwargs):
        """
        Count the number of columns for a key

        Parameters
        ----------
        key : str
            The key with which to count columns

        Returns
        -------
        int Count of columns
        """
        return self.column_family.get_count(*args, **kwargs)

    def get_range(self, *args, **kwargs):
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
        column_start = str
            Only fetch when a column is >= column_start
        column_finish = str
            Only fetch when a column is <= column_finish
        column_reversed = bool
            Fetch the columns in reverse order. Currently this does nothing
            because columns are converted into a dict.
        column_count = int
            Limit the number of columns fetched per key
        row_count = int
            Limit the number of rows fetched
        include_timestamp = bool
            If true, return a (value, timestamp) tuple for each column

        Returns
        -------
        iterator over Class instance
        """
        if 'columns' not in kwargs:
            kwargs['columns'] = self.columns.keys()
        for key, columns in self.column_family.get_range(*args, **kwargs):
            columns = combine_columns(self.columns, columns)
            yield create_instance(self.cls, key=key, **columns)

    def insert(self, instance, columns=None):
        """
        Insert or update columns for a key

        Parameters
        ----------
        instance : Class instance
            The key to insert or update the columns at
        columns : ['column']
            Limit the columns inserted to this list

        Returns
        -------
        int timestamp
        """
        insert_dict = {}
        if columns is None:
            columns = self.columns.keys()

        for column in columns:
            insert_dict[column] = self.columns[column].pack(instance.__dict__[column])

        return self.column_family.insert(instance.key, insert_dict)

    def remove(self, instance, column=None):
        """
        Remove this instance

        Parameters
        ----------
        instance : Class instance
            Remove the instance where the key is instance.key
        column : str
            If set, remove only this column

        Returns
        -------
        int timestamp
        """
        # Hmm, should we only remove the columns specified on construction?
        # It's slower, so we'll leave it out.
        return self.column_family.remove(instance.key, column)
