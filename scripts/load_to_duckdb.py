import duckdb
import os
from absl import app
from absl import flags

FLAGS = flags.FLAGS

flags.DEFINE_string('csv_dir', '~/datasets/job', 'Absolute or relative path to the directory to IMDB CSVs.')
flags.DEFINE_string('db_path', '~/data/duckdb/imdb/imdb.db',
                    'Absolute or relative path to the DuckDB database file.')

table_creation_sql_dict = {
    'company_type': 
        '''
            CREATE TABLE company_type (
                id INTEGER NOT NULL PRIMARY KEY,
                kind VARCHAR(32) NOT NULL COLLATE C
            );
        ''',
    'info_type': 
        '''
            CREATE TABLE info_type (
                id INTEGER NOT NULL PRIMARY KEY,
                info VARCHAR(32) NOT NULL COLLATE C
            );
        ''',
    'title':
        '''
            CREATE TABLE title (
                id INTEGER NOT NULL PRIMARY KEY,
                title TEXT NOT NULL COLLATE C,
                imdb_index VARCHAR(12) COLLATE C,
                kind_id INTEGER NOT NULL,
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
        '''
}

def Main(argv):
    del argv  # Unused.

    # Ensure the CSV directory exists.
    if not os.path.exists(FLAGS.csv_dir):
        raise FileNotFoundError(f"CSV directory '{FLAGS.csv_dir}' does not exist.")
    if not os.path.isdir(FLAGS.csv_dir):
        raise NotADirectoryError(f"Path '{FLAGS.csv_dir}' is not a directory.")
    
    print('Creating DuckDB tables and loading CSV files...')

    with duckdb.connect(FLAGS.db_path) as con:
        total_count, succeeded_count = 0, 0
        for table_name, create_sql in table_creation_sql_dict.items():
            # Check if the csv file exists
            csv_file_path = os.path.join(FLAGS.csv_dir, f"{table_name}.csv")
            if not os.path.exists(csv_file_path):
                print(f"CSV file '{csv_file_path}' does not exist, skipping table creation for '{table_name}'")
                continue

            print(f"Creating DuckDB table '{table_name}'")
            total_count += 1
            try:
                con.execute(f"DROP TABLE IF EXISTS {table_name}")
                con.execute(create_sql)
                con.sql(f'COPY {table_name} FROM \'{csv_file_path}\' (ESCAPE \'\\\')')
                succeeded_count += 1
            except Exception as e:
                print(f"Error creating DuckDB table '{table_name}': {e}")
        print(f'Total files processed: {total_count}, succeeded: {succeeded_count}, failed: {total_count - succeeded_count}')

if __name__ == '__main__':
    app.run(Main)