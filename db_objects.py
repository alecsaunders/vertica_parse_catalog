class Schema:
    def __init__(self):
        self.create_schema_statement = ''
        self.schema_name = ''
        self.include_privileges = False
        self.sequences = []
        self.tables = []
        self.external_tables = []
        self.flex_tables = []
        self.views = []
        self.column_comments = []
        self.dir_path = None

    def __str__(self):
        return str(self.schema_name)

    def all_tables(self):
        return self.tables + self.external_tables + self.flex_tables


class Sequence:
    def __init__(self, stmt):
        self.create_sequence_statement = stmt
        self.sequence_schema = stmt.split(' ')[2].split('.')[0].strip()


class Table:
    def __init__(self):
        self.create_table_statement = ''
        self.table_schema = ''
        self.table_name = ''
        self.alter_table_statements = []
        self.projections = []
        self.comments = []

    def __str__(self):
        return str(self.table_schema + '.' + self.table_name)


class ExternalTable(Table):
    def __init__(self):
        self.create_table_statement = ''
        self.table_schema = ''
        self.table_name = ''
        self.comments = []


class FlexTable(Table):
    def __init__(self):
        self.create_table_statement = ''
        self.table_schema = ''
        self.table_name = ''
        self.alter_table_statements = []
        self.projections = []
        self.comments = []


class Projection:
    def __init__(self):
        self.create_projection_statement = ''
        self.projection_schema = ''
        self.projection_basename = ''
        self.projection_name = ''
        self.anchor_table_name = ''

    def __str__(self):
        return str(self.projection_schema + '.' + self.projection_basename)


class View:
    def __init__(self):
        self.create_view_statement = ''
        self.view_schema = ''
        self.view_name = ''

    def __str__(self):
        return str('View: ' + self.view_schema + '.' + self.view_name)
