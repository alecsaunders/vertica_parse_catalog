import os
import sys

class SourceStructure:
    def __init__(self, catalog, source_top_dir):
        self.catalog = catalog
        self.source_top_dir = source_top_dir
        self.schemas_path = None

    def generate_source_dir_structure(self):
        self.create_schemas_dir()
        self.create_schemas()

    def create_schemas_dir(self):
        self.schemas_path = os.path.join(self.source_top_dir, '_SCEHMAS_')
        os.makedirs(self.schemas_path)

    def create_schemas(self):
        for s in self.catalog.schemas:
            self.populate_schema(s)

    def populate_schema(self, schema):
        self.create_schema_dir(schema)
        self.create_schema_files(schema)

    def create_schema_dir(self, schema):
        schema.dir_path = os.path.join(self.schemas_path, schema.schema_name)
        os.makedirs(schema.dir_path)

    def create_schema_files(self, schema):
        with open(schema.dir_path + '/CREATE_SCHEMA.sql', 'w') as create_schema:
            create_schema.write(schema.create_schema_statement.strip())
        with open(schema.dir_path + '/CREATE_SEQUENCE.sql', 'w') as create_sequence:
            for seq in schema.sequences:
                create_sequence.write(seq.create_sequence_statement + ';\n')
