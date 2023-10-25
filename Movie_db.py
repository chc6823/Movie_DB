import pandas as pd
import pymysql

# MySQL 연결
connection = pymysql.connect(host='127.0.0.1', database='movies', user='root', password='0000')
cursor = connection.cursor()

# Create tables if they don't exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS Movie (
    movie_id INT AUTO_INCREMENT,
    name_kor VARCHAR(255),
    name_eng VARCHAR(255),
    production_year INT,
    production_country VARCHAR(255),
    production_company VARCHAR(255),
    PRIMARY KEY (movie_id)
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Genre (
    genre_id INT AUTO_INCREMENT,
    genre_name VARCHAR(255),
    PRIMARY KEY (genre_id)
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Director (
    director_id INT AUTO_INCREMENT,
    name VARCHAR(255),
    PRIMARY KEY (director_id)
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Movie_Genre (
    movie_id INT,
    genre_id INT,
    PRIMARY KEY (movie_id, genre_id),
    FOREIGN KEY (movie_id) REFERENCES Movie (movie_id),
    FOREIGN KEY (genre_id) REFERENCES Genre (genre_id)
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Movie_Director (
    movie_id INT,
    director_id INT,
    PRIMARY KEY (movie_id, director_id),
    FOREIGN KEY (movie_id) REFERENCES Movie (movie_id),
    FOREIGN KEY (director_id) REFERENCES Director (director_id)
);
""")

# Commit the changes
connection.commit()

def process_data(cursor, file_path, sheet_name, header):
    column_names = ['영화명', '영화명(영문)', '제작연도', '제작국가', '유형', '장르', '제작상태', '감독', '제작사']
    data_df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl', header=header, names=column_names)

    # Replace NaN values
    data_df['제작연도'] = data_df['제작연도'].fillna(0)
    data_df = data_df.fillna('')

    # Prepare movie data
    movie_records = [(row['영화명'], row['영화명(영문)'], row['제작연도'], row['제작국가'], row['제작사']) for idx, row in data_df.iterrows()]

    # Prepare director data with split directors
    director_records = set()
    for directors in data_df['감독']:
        for director in directors.split(','):
            director = director.strip()
            director_records.add((director,))

    # Prepare genre data
    genre_records = set([(genre,) for genres in data_df['장르'].apply(lambda x: x.split(',')) for genre in genres])

    # Disable autocommit
    connection.autocommit(False)

    try:
        # Insert movie data
        insert_movie_data(cursor, movie_records)

        # Insert director data
        insert_director_data(cursor, director_records)

        # Insert genre data
        insert_genre_data(cursor, genre_records)

        # Prepare and insert movie_genre data
        movie_genre_records = []
        for idx, row in data_df.iterrows():
            movie_id = idx + 1  # Assuming movie_id starts from 1
            genres = row['장르'].split(',')
            for genre in genres:
                genre = genre.strip()
                genre_id = get_genre_id(cursor, genre)
                if genre_id is not None:
                    movie_genre_records.append((movie_id, genre_id))

        insert_movie_genre_data(cursor, movie_genre_records)

        # Prepare and insert movie_director data
        movie_director_records = []
        existing_records = set()  # To track existing movie_director records

        for idx, row in data_df.iterrows():
            movie_id = idx + 1  # Assuming movie_id starts from 1
            directors = row['감독'].split(',')
            for director in directors:
                director = director.strip()
                director_id = get_director_id(cursor, director)
                if director_id is not None:
                    movie_director = (movie_id, director_id)
                    # Check if the movie_director record already exists
                    if movie_director not in existing_records:
                        movie_director_records.append(movie_director)
                        existing_records.add(movie_director)
        print(movie_director_records)
        insert_movie_director_data(cursor, movie_director_records)

        # Commit the changes
        connection.commit()
        print("Data has been successfully processed and inserted.")
    except Exception as e:
        print(f"Error occurred during data processing: {str(e)}")
        connection.rollback()

    # Enable autocommit
    connection.autocommit(True)

def get_genre_id(cursor, genre_name):
    try:
        sql = "SELECT genre_id FROM Genre WHERE genre_name = %s"
        cursor.execute(sql, (genre_name,))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            return None
    except Exception as e:
        print(f"Error occurred when retrieving genre ID: {str(e)}")
        return None

def get_director_id(cursor, director_name):
    try:
        sql = "SELECT director_id FROM Director WHERE name = %s"
        cursor.execute(sql, (director_name,))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            return None
    except Exception as e:
        print(f"Error occurred when retrieving director ID: {str(e)}")
        return None

def insert_movie_data(cursor, movie_records):
    try:
        movie_sql = "INSERT IGNORE INTO Movie (name_kor, name_eng, production_year, production_country, production_company) VALUES (%s, %s, %s, %s, %s)"
        cursor.executemany(movie_sql, movie_records)
        connection.commit()
        print(f"Inserted {len(movie_records)} movies")
    except Exception as e:
        print(f"Error occurred when inserting movie data: {str(e)}")
        connection.rollback()

def insert_director_data(cursor, director_records):
    try:
        director_sql = "INSERT IGNORE INTO Director (name) VALUES (%s)"
        cursor.executemany(director_sql, director_records)
        connection.commit()
        print(f"Inserted {len(director_records)} directors")
    except Exception as e:
        print(f"Error occurred when inserting director data: {str(e)}")
        connection.rollback()

def insert_genre_data(cursor, genre_records):
    try:
        genre_sql = "INSERT IGNORE INTO Genre (genre_name) VALUES (%s)"
        cursor.executemany(genre_sql, genre_records)
        connection.commit()
        print(f"Inserted {len(genre_records)} genres")
    except Exception as e:
        print(f"Error occurred when inserting genre data: {str(e)}")
        connection.rollback()

def insert_movie_genre_data(cursor, movie_genre_records):
    try:
        movie_genre_sql = "INSERT IGNORE INTO Movie_Genre (movie_id, genre_id) VALUES (%s, %s)"
        inserted_records = 0  # To track the number of inserted records

        for record in movie_genre_records:
            try:
                cursor.execute(movie_genre_sql, record)
                inserted_records += 1
            except Exception as e:
                print(f"Error occurred when inserting movie_genre record {record}: {str(e)}")
        connection.commit()
        print(f"Inserted {inserted_records} movie_genre records")
    except Exception as e:
        print(f"Error occurred when inserting movie_genre data: {str(e)}")
        connection.rollback()

def insert_movie_director_data(cursor, movie_director_records):
    try:
        movie_director_sql = "INSERT IGNORE INTO Movie_Director (movie_id, director_id) VALUES (%s, %s)"
        inserted_records = 0  # To track the number of inserted records

        for record in movie_director_records:
            try:
                cursor.execute(movie_director_sql, record)
                inserted_records += 1
                print(f"Inserted movie_director record: {record}")
            except Exception as e:
                print(f"Error occurred when inserting movie_director record {record}: {str(e)}")
        connection.commit()
        print(f"Inserted {inserted_records} movie_director records")
    except Exception as e:
        print(f"Error occurred when inserting movie_director data: {str(e)}")
        connection.rollback()


# Process data
process_data(cursor, 'movies.xlsx', '영화정보 리스트', 4)
process_data(cursor, 'movies.xlsx', '영화정보 리스트_2', 0)

# Close cursor and connection
cursor.close()
connection.close()

#===============

