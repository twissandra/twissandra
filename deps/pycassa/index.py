from cassandra.ttypes import IndexClause, IndexExpression, IndexOperator

def create_index_clause(expr_list, start_key='', count=100):
    """
    Constructs an IndexClause for use with get_indexed_slices()

    Parameters
    ----------
    expr_list : [IndexExpression]
        A list of IndexExpressions to match
    start_key : str
        The key to begin searching from
    count : int
        The number of results to return
    """
    return IndexClause(expressions=expr_list, start_key=start_key,
                       count=count)

def create_index_expression(column_name, value, op=IndexOperator.EQ):
    """
    Constructs an IndexExpression to use with an IndexClause

    Parameters
    ----------
    column_name : str
        Name of an indexed or non indexed column
    value : str
        The value that will be compared to column values using op
    op : IndexOperator
        The binary operator to apply to column values and 'value'
    """
    return IndexExpression(column_name=column_name, op=op, value=value)
