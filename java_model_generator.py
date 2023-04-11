from argparse import Namespace
from pyodbc import Connection
from pyodbc import Row
from re import sub
from typing import List, Set
import argparse
import os
import pathlib
import pyodbc


JAVA_TYPES = {
    'INT': {'type': 'Integer', 'class': None},
    'INTEGER': {'type': 'Integer', 'class': None},
    'BIGINT': {'type': 'Long', 'class': None},
    'SMALLINT': {'type': 'Short', 'class': None},
    'REAL': {'type': 'Float', 'class': None},
    'FLOAT': {'type': 'Float', 'class': None},
    'DOUBLE': {'type': 'Double', 'class': None},
    'DECIMAL': {'type': 'BigDecimal', 'class': 'java.math.BigDecimal;'},
    'NUMERIC': {'type': 'BigDecimal', 'class': 'java.math.BigDecimal;'},
    'NCHAR': {'type': 'String', 'class': None},
    'CHAR': {'type': 'String', 'class': None},
    'VARCHAR': {'type': 'String', 'class': None},
    'NVARCHAR': {'type': 'String', 'class': None},
    'TINYINT': {'type': 'Byte', 'class': None},
    'BIT': {'type': 'Boolean', 'class': None},
    'DATE': {'type': 'Date', 'class': 'java.util.Date;'},
    'DATETIME': {'type': 'Date', 'class': 'java.util.Date;'},
    'BINARY': {'type': 'Byte[]', 'class': None},
    'VARBINARY': {'type': 'Byte[]', 'class': None},
    'IMAGE': {'type': 'Byte[]', 'class': None},
}


def get_tables_query(table: str) -> str:
    query = f'SELECT t.TABLE_NAME FROM INFORMATION_SCHEMA.TABLES AS t WHERE t.TABLE_NAME = \'{table}\'' \
        if table else 'SELECT t.TABLE_NAME FROM INFORMATION_SCHEMA.TABLES AS t'
    return query


def get_columns_and_types_query(table: str) -> str:
    return f'SELECT c.COLUMN_NAME, c.DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS AS c WHERE c.TABLE_NAME = \'{table}\''


def get_imports_string() -> str:
    return """\

import jakarta.persistence.Entity;
import java.io.Serial;
import java.io.Serializable;
import jakarta.persistence.Column;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

"""


def get_class_annotations() -> str:
    return """\
@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog='java-model-generator',
        description='Generates entity model classes for Java/Hibernate based on a database\'s INFORMATION_SCHEMA'
    )
    parser.add_argument(
        '-c',
        '--connection_string',
        help='Connection string for your database. \
            Use instead of -s, -d, -u, and -p flags. \
            For more information see https://www.connectionstrings.com \
            and https://www.connectionstrings.com/formating-rules-for-connection-strings/'
    )
    parser.add_argument(
        '-d',
        '--driver',
        help='Driver for target SQL server. Use instead of -c, --connection_string flag'
    )
    parser.add_argument(
        '-s',
        '--server',
        help='Server to connect to. Use instead of -c, --connection_string flag'
    )
    parser.add_argument(
        '-n',
        '--database',
        help='Database to connect to. Use instead of -c, --connection_string flag'
    )
    parser.add_argument(
        '-u',
        '--username',
        help='Username to use when connecting to server. Use instead of -c, --connection_string flag'
    )
    parser.add_argument(
        '-p',
        '--password',
        help='Password for username used to connect to server. Use instead of -c, --connection_string flag'
    )
    parser.add_argument(
        '-j',
        '--package',
        required=True,
        help='package name to insert into new model classes'
    )
    parser.add_argument(
        '-i',
        '--indentation',
        default='    ',
        help='Indentation formatting for class members'
    )
    parser.add_argument(
        '-o',
        '--output',
        default=os.path.join(f'{pathlib.Path(__file__).parent.resolve()}', 'models'),
        help='Directory to output generated .java files'
    )
    parser.add_argument(
        '-t',
        '--table',
        help='Specify a TABLE_NAME to generate a model class for. Default is to generate a model class for every \
              table in a database\'s INFORMATION_SCHEMA'
    )
    return parser.parse_args()


def get_java_type_for_type(sql_data_type: str) -> str:
    return JAVA_TYPES.get(sql_data_type.upper()).get('type')


def get_java_class_for_type(sql_data_type: str) -> str:
    return JAVA_TYPES.get(sql_data_type.upper()).get('class')


def get_db_connection_from_args(driver: str, server: str, database: str, username: str = '',
                                password: str = '') -> Connection:
    connection_string = 'Driver={' + f'{driver}' + '};' + f'Server={server};Database={database};'
    if username:
        connection_string += connection_string + f'UID={username};'
    if password:
        connection_string += connection_string + f'PWD={password}'
    return pyodbc.connect(connection_string)


def get_db_connection_from_connection_string(connection_string: str) -> Connection:
    return pyodbc.connect(connection_string)


def validate_args_usage(arguments: Namespace) -> None:
    if not arguments.connection_string and not (arguments.driver and arguments.server and arguments.database):
        argparse.ArgumentError(None,
                               'Please provide database connection information via either -c OR -d, -s, -n, flags')


def camel_case(string: str, lower=False) -> str:
    string = sub(r"(_|-)+", " ", string).title().replace(" ", "")
    return ''.join([string[0].lower(), string[1:]]) if lower else string


def get_db_tables(db_connection: Connection, query: str) -> List[Row]:
    with db_connection.cursor() as cursor:
        return cursor.execute(query).fetchall()


def get_db_columns_and_types(db_connection: Connection, query: str) -> List[Row]:
    with db_connection.cursor() as cursor:
        return cursor.execute(query).fetchall()


def get_type_imports_statements(type_classes: Set) -> str:
    imports = '\n'.join(f'import {i}' for i in type_classes if i)
    if imports:
        return imports + '\n'


def create_output_directory(directory: str) -> None:
    normalised_directory_path = os.path.normpath(directory)
    if not os.path.exists(normalised_directory_path):
        os.makedirs(directory)


def write_class_to_file(table: str, columns_types: List[Row], package: str, indent: str, directory: str) -> None:
    type_imports = {get_java_class_for_type(java_type[1]) for java_type in columns_types}
    create_output_directory(directory)
    with open(os.path.join(f'{directory}', f'{camel_case(table)}.java'), 'w') as file:
        file.write(f'package {package};\n')
        file.write(get_imports_string())
        file.write(get_type_imports_statements(type_imports))
        file.write('\n')
        file.write(get_class_annotations())
        file.write(f'@Table(name = "{table}")\n')
        file.write(f'public class {camel_case(table)} implements Serializable ')
        file.write('{\n')
        file.write('\n')
        file.write(f'{indent}@Serial\n')
        file.write(f'{indent}private static final long serialVersionUID = 1L;\n')
        file.write('\n')
        file.write(f'{indent}@Id\n')
        for column_type in columns_types:
            column = column_type[0]
            java_type = get_java_type_for_type(column_type[1])
            file.write(f'{indent}@Column(name = "{column}")\n')
            file.write(f'{indent}private {java_type} ')
            file.write(f'{camel_case(column_type[0], True)};\n')
            file.write('\n')
        file.write('}')


def build_model_class_loop(db_connection: Connection, package: str, indent: str, directory: str, table: str) -> None:
    tables_query = get_tables_query(table)
    tables = get_db_tables(db_connection, tables_query)
    for table in tables:
        table_name = table[0]
        columns_and_types_query = get_columns_and_types_query(table_name)
        columns_and_types = get_db_columns_and_types(db_connection, columns_and_types_query)
        write_class_to_file(table_name, columns_and_types, package, indent, directory)


if __name__ == '__main__':
    args = parse_args()
    validate_args_usage(args)
    if args.connection_string:
        connection = get_db_connection_from_connection_string(args.connection_string)
    else:
        connection = get_db_connection_from_args(args.driver, args.server, args.database, args.username, args.password)
    build_model_class_loop(connection, args.package, args.indentation, args.output, args.table)
