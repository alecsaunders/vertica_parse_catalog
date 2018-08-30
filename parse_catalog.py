#!/usr/bin/env python3

import os
import sys
import db_objects
import projection_parser
from db_source_structure import SourceStructure


class Catalog:
    def __init__(self):
        self.catalog_script = None
        self.schemas = []
        pass

    def create_directory_structure(self):
        ss = SourceStructure(self, 'full_catalog')
        ss.generate_source_dir_structure()

    def parse_catalog(self):
        cat_objs = self.catalog_script.split(';')

        public_schema = db_objects.Schema()
        public_schema.schema_name = 'public'
        self.schemas = [public_schema] + list(filter(None, [self.parse_schemas(o) for o in cat_objs]))
        [self.parse_other_objects(o) for o in cat_objs]

    def parse_schemas(self, object):
        obj_scpt = ' '.join(object.split())
        if obj_scpt.lower().startswith('create schema'):
            s = db_objects.Schema()
            s.create_schema_statement = object
            s.schema_name = object.split(' ')[2]
            return s
        return None

    def parse_other_objects(self, object):
        obj = self.format_object(object)
        obj_scpt = ' '.join(obj.split())

        if obj_scpt.lower().startswith('create schema'):
            return
        if obj_scpt.lower().startswith('create sequence'):
            self.parse_sequence(obj)
        elif obj_scpt.lower().startswith('create table'):
            self.parse_table(obj)
        elif obj_scpt.lower().startswith('create managed external table'):
            self.parse_mngd_ex_table(obj)
        elif obj_scpt.lower().startswith('create flex table'):
            self.parse_flex_table(obj)
        elif obj_scpt.lower().startswith('create temporary table'):
            pass
        elif obj_scpt.lower().startswith('alter table'):
            self.parse_alter_table(obj)
        elif obj_scpt.lower().startswith('create projection'):
            self.parse_projection(obj)
        elif obj_scpt.lower().startswith('comment on table'):
            self.parse_table_comment(obj)
        elif obj_scpt.lower().startswith('comment on column'):
            self.parse_column_comment(obj)
        elif obj_scpt.lower().startswith('create view'):
            self.parse_view(obj)
        elif obj_scpt.lower().startswith('create function'):
            obj = obj + ';\nEND;'
            pass
        elif obj_scpt.lower() == 'end':
            # This is the 'END' transaction for functions and can be ignored
            pass
        elif obj_scpt.lower().startswith('select mark_design_ksafe'):
            # The k-safety should be determined by the database, ignore this
            pass
        else:
            if obj:
                print("unhandled object!!")
                print(obj)

    def format_object(self, obj):
        obj = obj.strip()
        while '  ' in obj:
            obj = obj.replace('  ', ' ')
        return obj

    def parse_sequence(self, seq):
        sequence = db_objects.Sequence(seq)
        [self.match_sequence_to_schema(s, sequence) for s in self.schemas]

    def match_sequence_to_schema(self, schema, seq):
        if seq.sequence_schema == schema.schema_name:
            schema.sequences.append(seq)

    def parse_table(self, table):
        table_schema, table_name = table.replace('(', '').split(' ')[2].strip().split('.')
        t = db_objects.Table()
        t.table_schema = table_schema
        t.table_name = table_name

        parent_schema = self.match_object_to_schema(table_schema)
        parent_schema.tables.append(t)

    def parse_mngd_ex_table(self, ex_table):
        table_schema, table_name = ex_table.replace('(', ' ').split(' ')[4].strip().split('.')
        parent_schema = self.match_object_to_schema(table_schema)
        ex_table = db_objects.ExternalTable()
        ex_table.table_schema = table_schema
        ex_table.table_name = table_name
        parent_schema.external_tables.append(ex_table)

    def parse_flex_table(self, flex_table):
        table_schema, table_name = flex_table.replace('(', ' ').split(' ')[3].strip().split('.')
        ft = db_objects.FlexTable()
        ft.table_schema = table_schema
        ft.table_name = table_name

        ### !!! ADD CREATE TABLE STATMENT TO FLEX TABLE OBJECT !!! ###

        parent_schema = self.match_object_to_schema(table_schema)
        parent_schema.flex_tables.append(ft)

    def parse_alter_table(self, alter):
        alter_schema, alter_table = alter.replace('(', ' ').split(' ')[2].strip().split('.')
        parent_schema = self.match_object_to_schema(alter_schema)
        table = self.match_object_to_table(parent_schema.all_tables(), alter_table)
        table.alter_table_statements.append(alter)

    def parse_projection(self, proj):
        proj_parser = projection_parser.ProjParser()
        proj_parser.raw_proj = proj
        proj_parser.parse_projection()
        proj_parser.recompile_projection()
        new_proj = proj_parser.recompiled_projection

        proj = db_objects.Projection()
        proj.create_projection_statement = new_proj
        proj.projection_schema = proj_parser.projection_schema
        proj.projection_basename = proj_parser.projection_basename
        proj.projection_name = proj_parser.projection_name
        proj.anchor_table_name = proj_parser.from_schema + '.' + proj_parser.from_table

        parent_schema = self.match_object_to_schema(proj_parser.from_schema)
        table = self.match_object_to_table(parent_schema.all_tables(), proj_parser.from_table)

        if table:
            table.projections.append(proj)

    def parse_table_comment(self, comment):
        comment_full_table = comment.split()[3]
        c_schema, c_table = comment_full_table.split('.')
        parent_schema = self.match_object_to_schema(c_schema)
        table = self.match_object_to_table(parent_schema.all_tables(), c_table)
        table.comments.append(comment)

    def parse_column_comment(self, comment):
        comment_full_column = comment.split()[3]
        c_schema, c_proj_name, c = comment_full_column.split('.')
        parent_schema = self.match_object_to_schema(c_schema)
        parent_schema.column_comments.append(comment)

    def parse_view(self, view):
        full_view_name = view.split()[2]
        view_schema, view_name = full_view_name.split('.')
        v = db_objects.View()
        v.view_schema = view_schema
        v.view_name = view_name
        v.create_view_statement = view

        parent_schema = self.match_object_to_schema(view_schema)
        parent_schema.views.append(v)

    # UTILITY FUNCTIONS
    def match_object_to_schema(self, obj_schema_name):
        for s in self.schemas:
            if s.schema_name == obj_schema_name:
                return s

    def match_object_to_table(self, tables, object_table_name):
        for t in tables:
            if t.table_name == object_table_name:
                return t


if __name__ == '__main__':
    cat = open('catalog.sql', 'r').read()

    c = Catalog()
    c.catalog_script = cat
    c.parse_catalog()
    c.create_directory_structure()
