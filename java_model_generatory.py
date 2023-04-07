from re import sub
from typing import List, Set, Tuple
import pyodbc
import argparse
import pathlib
import os

JAVA_TYPES = {
    'INT': { 'type': 'Integer', 'class': 'java.lang.Integer;' },
    'INTEGER': { 'type': 'Integer', 'class': 'java.lang.Integer;' },
    'BIGINT': { 'type': 'Long', 'class': 'java.lang.Long;' },
    'SMALLINT': { 'type': 'Short', 'class': 'java.lang.Short;' },
    'REAL': { 'type': 'Float', 'class': 'java.lang.Float;' },
    'FLOAT': { 'type': 'Float', 'class': 'java.lang.Float;' },
    'DOUBLE': { 'type': 'Double', 'class': 'java.lang.Double;' },
    'DECIMAL': { 'type': 'BigDecimal', 'class': 'java.math.BigDecimal;' },
    'NUMERIC': { 'type': 'BigDecimal', 'class': 'java.math.BigDecimal;' },
    'NCHAR': { 'type': 'String', 'class': 'java.lang.String;' },
    'CHAR': { 'type': 'String', 'class': 'java.lang.String;' },
    'VARCHAR': { 'type': 'String', 'class': 'java.lang.String;' },
    'NVARCHAR': { 'type': 'String', 'class': 'java.lang.String;' },
    'TINYINT': { 'type': 'Byte', 'class': 'java.lang.Byte;' },
    'BIT': { 'type': 'Boolean', 'class': 'java.lang.Boolean;' },
    'DATE': { 'type': 'Date', 'class': 'java.util.Date;' },
    'DATETIME': { 'type': 'Date', 'class': 'java.util.Date;' },
    'BINARY': { 'type': 'Byte[]', 'class': 'java.lang.Byte' },
    'VARBINARY': { 'type': 'Byte[]', 'class': 'java.lang.Byte' },
    'IMAGE': { 'type': 'Byte[]', 'class': 'java.lang.Byte' },
}

TABLES_QUERY = 'SELECT t.TABLE_NAME FROM INFORMATION_SCHEMA.TABLES AS t'
COLUMNS_QUERY = f'SELECT c.COLUMN_NAME, c.DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS AS c WHERE c.TABLE_NAME = \'{{}}\''

def get_imports_string() -> str:
    return  """\

import jakarta.persistence.Entity;
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
        help='Driver for target SQL server'
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
    return parser.parse_args()


def get_java_type_for_type(sql_data_type: str) -> str:
    return JAVA_TYPES.get(sql_data_type.upper()).get('type')


def get_java_class_for_type(sql_data_type: str) -> str:
    return JAVA_TYPES.get(sql_data_type.upper()).get('class')


def get_db_connection_from_args(driver: str, server: str, database: str, username: str = '', password: str = '') -> pyodbc.Connection:
    connection_string = 'Driver={' + f'{driver}' + '};' + f'Server={server};Database={database};' 
    if username:
        connection_string += connection_string + f'UID={username};'
    if password:
        connection_string += connection_string + f'PWD={password}'
    return pyodbc.connect(connection_string)


def get_db_connection_from_connection_string(connection_string: str) -> pyodbc.Connection:
    return pyodbc.connect(connection_string)


def validate_args_usage(args: argparse.Namespace) -> None:
    if not args.connection_string and not (args.driver and args.server and args.database):
        argparse.ArgumentError(None, 'Please provide database connection information via either -c OR -d, -s, -n, flags')


def camel_case(string: str, lower=False) -> str:
  string = sub(r"(_|-)+", " ", string).title().replace(" ", "")
  return ''.join([string[0].lower(), string[1:]]) if lower else string


def get_db_tables(connection: pyodbc.Connection) -> List[Tuple]:
    with connection.cursor() as cursor:
        return cursor.execute(TABLES_QUERY).fetchall()


def get_columns_and_types(connection: pyodbc.Connection, table: str) -> List[Tuple]:
    with connection.cursor() as cursor:
        return cursor.execute(COLUMNS_QUERY.format(table)).fetchall()


def add_type_imports(type_classes: Set) -> str:
    imports = '\n'.join(f'import {i}' for i in type_classes)
    if imports:
        return get_imports_string() + '\n' + imports + '\n'


def create_output_directory(directory: str) -> None:
    normalised_directory_path = os.path.normpath(directory) 
    if not os.path.exists(normalised_directory_path):
        os.makedirs(directory)

def write_class_to_file(table: str, columns_types: List[Tuple], package: str, indent: str, directory: str) -> None:
    type_imports = { get_java_class_for_type(java_type[1]) for java_type in columns_types } 
    create_output_directory(directory)
    with open(os.path.join(f'{directory}', f'{table}.java'), 'w') as file:
        file.write(f'package {package};\n') 
        file.write(add_type_imports(type_imports))
        file.write('\n')
        file.write(get_class_annotations())
        file.write(f'@Table(name = "{table}")\n')
        file.write(f'public class {camel_case(table)} implements Serializable ')
        file.write('{\n')
        file.write('\n')
        file.write(f'{indent}private static final long serialVersionUID = 1L;\n')
        file.write('\n')
        file.write(f'{indent}@Id\n')
        for column_type in columns_types:
            column = column_type[0]
            java_type = get_java_type_for_type(column_type[1])
            file.write(f'{indent}@Column(name = "{column}")\n')
            file.write(f'{indent}private {java_type} ') 
            file.write(f'{camel_case(column_type[0], True)}\n')
            file.write('\n')
        file.write('}')
    

def build_model_class_loop(connection: pyodbc.Connection, package: str, indent: str, directory: str) -> None:
    tables = get_db_tables(connection)
    for table in tables:
        table_name = table[0]
        columns_and_types = get_columns_and_types(connection, table_name)
        write_class_to_file(table_name, columns_and_types, package, indent, directory)


if __name__ == '__main__':
    args = parse_args()
    validate_args_usage(args)
    if args.connection_string:
        connection = get_db_connection_from_connection_string(args.connection_string)
    else:
        connection = get_db_connection_from_args(args.driver, args.server, args.database, args.username, args.password)
    build_model_class_loop(connection, args.package, args.indentation, args.output)
