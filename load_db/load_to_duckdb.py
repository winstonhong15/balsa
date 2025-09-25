import duckdb
import os
from pathlib import Path
from absl import app
from absl import flags

FLAGS = flags.FLAGS

home_dir = str(Path.home())
flags.DEFINE_string('csv_dir', os.path.join(home_dir, 'datasets', 'job'), 'Absolute or relative path to the directory to IMDB CSVs.')
flags.DEFINE_string('db_path', os.path.join(home_dir, 'imdb.db'),
                    'Absolute or relative path to the DuckDB database file.')

table_creation_sql_dict = {
    'aka_name':
        '''
            CREATE TABLE aka_name (
                id INTEGER NOT NULL PRIMARY KEY REFERENCES name(id),
                person_id INTEGER NOT NULL,
                name TEXT NOT NULL COLLATE C,
                imdb_index VARCHAR(12) COLLATE C,
                name_pcode_cf VARCHAR(5) COLLATE C,
                name_pcode_nf VARCHAR(5) COLLATE C,
                surname_pcode VARCHAR(5) COLLATE C,
                md5sum VARCHAR(32) COLLATE C
            );
        ''',
    'aka_title':
        '''
            CREATE TABLE aka_title (
                id INTEGER NOT NULL PRIMARY KEY,
                movie_id INTEGER NOT NULL,
                title TEXT NOT NULL collate C,
                imdb_index VARCHAR(12) collate C,
                kind_id INTEGER NOT NULL,
                production_year INTEGER,
                phonetic_code VARCHAR(5) collate C,
                episode_of_id INTEGER,
                season_nr INTEGER,
                episode_nr INTEGER,
                note TEXT collate C,
                md5sum VARCHAR(32) collate C
            );
        ''',
    'cast_info':
        '''
            CREATE TABLE cast_info (
                id INTEGER NOT NULL PRIMARY KEY,
                person_id INTEGER NOT NULL,
                movie_id INTEGER NOT NULL REFERENCES title(id),
                person_role_id INTEGER REFERENCES char_name(id),
                note TEXT COLLATE C,
                nr_order INTEGER,
                role_id INTEGER NOT NULL REFERENCES role_type(id)
            );
        ''',
    'char_name':
        '''
            CREATE TABLE char_name (
                id INTEGER NOT NULL PRIMARY KEY,
                name TEXT NOT NULL COLLATE C,
                imdb_index VARCHAR(12) COLLATE C,
                imdb_id INTEGER,
                name_pcode_nf VARCHAR(5) COLLATE C,
                surname_pcode VARCHAR(5) COLLATE C,
                md5sum VARCHAR(32) COLLATE C
            );
        ''',
    'comp_cast_type':
        '''
            CREATE TABLE comp_cast_type (
                id INTEGER NOT NULL PRIMARY KEY,
                kind VARCHAR(32) NOT NULL COLLATE C
            );
        ''',
    'company_name':
        '''
            CREATE TABLE company_name (
                id INTEGER NOT NULL PRIMARY KEY,
                name TEXT NOT NULL COLLATE C,
                country_code VARCHAR(255) COLLATE C,
                imdb_id INTEGER,
                name_pcode_nf VARCHAR(5) COLLATE C,
                name_pcode_sf VARCHAR(5) COLLATE C,
                md5sum VARCHAR(32) COLLATE C
            );
        ''',
    'company_type': 
        '''
            CREATE TABLE company_type (
                id INTEGER NOT NULL PRIMARY KEY,
                kind VARCHAR(32) NOT NULL COLLATE C
            );
        ''',
    'complete_cast':
        '''
            CREATE TABLE complete_cast (
                id INTEGER NOT NULL PRIMARY KEY,
                movie_id INTEGER REFERENCES title (id),
                subject_id INTEGER NOT NULL REFERENCES comp_cast_type (id),
                status_id INTEGER NOT NULL REFERENCES comp_cast_type (id)
            );
        ''',
    'info_type': 
        '''
            CREATE TABLE info_type (
                id INTEGER NOT NULL PRIMARY KEY,
                info VARCHAR(32) NOT NULL COLLATE C
            );
        ''',
    'keyword':
        '''
            CREATE TABLE keyword (
                id INTEGER NOT NULL PRIMARY KEY,
                keyword TEXT NOT NULL COLLATE C,
                phonetic_code VARCHAR(5) COLLATE C
            );
        ''',
    'kind_type':
        '''
            CREATE TABLE kind_type (
                id INTEGER NOT NULL PRIMARY KEY,
                kind VARCHAR(15) NOT NULL COLLATE C
            );
        ''',
    'link_type':
        '''
            CREATE TABLE link_type (
                id INTEGER NOT NULL PRIMARY KEY,
                link VARCHAR(32) NOT NULL COLLATE C
            );
        ''',
    'movie_companies':
        '''
            CREATE TABLE movie_companies (
                id INTEGER NOT NULL PRIMARY KEY,
                movie_id INTEGER NOT NULL,
                company_id INTEGER NOT NULL,
                company_type_id INTEGER NOT NULL,
                note TEXT COLLATE C,
                FOREIGN KEY (movie_id) REFERENCES title (id)
            );
        ''',
    'movie_info':
        '''
            CREATE TABLE movie_info (
                id integer NOT NULL PRIMARY KEY,
                movie_id integer NOT NULL REFERENCES title (id),
                info_type_id integer NOT NULL REFERENCES info_type (id),
                info text NOT NULL collate "C",
                note text collate "C"
            );
        ''',
    'movie_info_idx':
        '''
            CREATE TABLE movie_info_idx (
                id INTEGER NOT NULL PRIMARY KEY,
                movie_id INTEGER NOT NULL,
                info_type_id INTEGER NOT NULL,
                info TEXT NOT NULL COLLATE C,
                note TEXT COLLATE C,
                FOREIGN KEY (movie_id) REFERENCES title (id),
                FOREIGN KEY (info_type_id) REFERENCES info_type (id)
            );
        ''',
    'movie_keyword':
        '''
            CREATE TABLE movie_keyword (
                id INTEGER NOT NULL PRIMARY KEY,
                movie_id INTEGER NOT NULL REFERENCES title (id),
                keyword_id INTEGER NOT NULL REFERENCES keyword (id)
            );
        ''',
    'movie_link':
        '''
            CREATE TABLE movie_link (
                id INTEGER NOT NULL PRIMARY KEY,
                movie_id INTEGER NOT NULL REFERENCES title (id),
                linked_movie_id INTEGER NOT NULL,
                link_type_id INTEGER NOT NULL REFERENCES link_type (id)
            );
        ''',
    'name':
        '''
            CREATE TABLE name (
                id INTEGER NOT NULL PRIMARY KEY,
                name TEXT NOT NULL COLLATE C,
                imdb_index VARCHAR(12) COLLATE C,
                imdb_id INTEGER,
                gender VARCHAR(1) COLLATE C,
                name_pcode_cf VARCHAR(5) COLLATE C,
                name_pcode_nf VARCHAR(5) COLLATE C,
                surname_pcode VARCHAR(5) COLLATE C,
                md5sum VARCHAR(32) COLLATE C
            );
        ''',
    'person_info':
        '''
            CREATE TABLE person_info (
                id INTEGER NOT NULL PRIMARY KEY,
                person_id INTEGER NOT NULL REFERENCES name (id),
                info_type_id INTEGER NOT NULL REFERENCES info_type (id),
                info TEXT NOT NULL COLLATE C,
                note TEXT COLLATE C
            );
        ''',
    'role_type':
        '''
            CREATE TABLE role_type (
                id INTEGER NOT NULL PRIMARY KEY,
                role VARCHAR(32) NOT NULL COLLATE C
            );
        ''',
    'title':
        '''
            CREATE TABLE title (
                id INTEGER NOT NULL PRIMARY KEY,
                title TEXT NOT NULL COLLATE C,
                imdb_index VARCHAR(12) COLLATE C,
                kind_id INTEGER NOT NULL REFERENCES kind_type (id),
                production_year INTEGER,
                imdb_id INTEGER,
                phonetic_code VARCHAR(5) COLLATE C,
                episode_of_id INTEGER,
                season_nr INTEGER,
                episode_nr INTEGER,
                series_years VARCHAR(49) COLLATE C,
                md5sum VARCHAR(32) COLLATE C
            );
        ''',
}

table_creation_order = [
    ['aka_title', 'char_name', 'comp_cast_type', 'company_name', 'company_type', 'info_type', 'keyword', 'kind_type', 'link_type', 'name', 'role_type'],
    ['aka_name', 'person_info', 'title'],
    ['cast_info', 'complete_cast', 'movie_companies', 'movie_info', 'movie_info_idx', 'movie_keyword', 'movie_link']
]

def Main(argv):
    del argv  # Unused.

    # Ensure the CSV directory exists.
    if not os.path.exists(FLAGS.csv_dir):
        raise FileNotFoundError(f"CSV directory '{FLAGS.csv_dir}' does not exist.")
    if not os.path.isdir(FLAGS.csv_dir):
        raise NotADirectoryError(f"Path '{FLAGS.csv_dir}' is not a directory.")
    
    print('Creating DuckDB tables and loading CSV files...')

    with duckdb.connect(FLAGS.db_path) as con:
        # Drop table in reverse order
        for tables in table_creation_order[::-1]:
            for table in tables:
                con.execute(f"DROP TABLE IF EXISTS {table}")

        # Create and populate tables
        total_count, succeeded_count = 0, 0
        for tables in table_creation_order:
            for table in tables:
                # Check if the csv file exists
                csv_file_path = os.path.join(FLAGS.csv_dir, f"{table}.csv")
                if not os.path.exists(csv_file_path):
                    print(f"CSV file '{csv_file_path}' does not exist, skipping table creation for '{table}'")
                    continue

                print(f"Creating DuckDB table '{table}'")
                total_count += 1
                try:
                    con.execute(table_creation_sql_dict[table])
                    con.sql(f'COPY {table} FROM \'{csv_file_path}\' (ESCAPE \'\\\')')
                    succeeded_count += 1
                except Exception as e:
                    print(f"Error creating DuckDB table '{table}': {e}")
        print(f'Total files processed: {total_count}, succeeded: {succeeded_count}, failed: {total_count - succeeded_count}')

        index_file_path = os.path.join(os.getcwd(), 'fkindexes.sql')
        if os.path.exists(index_file_path):
            print('Adding index')
            with open(index_file_path) as f:
                for create_index_sql in f:
                    try:
                        con.sql(create_index_sql)
                    except Exception as e:
                        print(f"Error creating index: {e}")

if __name__ == '__main__':
    app.run(Main)