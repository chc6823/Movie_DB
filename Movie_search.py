import pymysql

# MySQL 연결
connection = pymysql.connect(host='127.0.0.1', database='movies', user='root', password='0000')

def search_movies():
    print("=== 영화 검색 ===")
    print("검색 조건을 입력하세요.")

    # 영화 제목
    name = input("영화 제목: ")

    # 제작 연도
    production_year = input("제작 연도: ")

    # 제작 국가
    production_country = input("제작 국가: ")

    # 장르
    genre = input("장르: ")

    # 감독
    director = input("감독: ")

    # 쿼리 조건 및 매개변수
    conditions = []
    params = []

    if name != ".":
        conditions.append("name_kor LIKE %s")  # 부분 일치 검색을 위해 LIKE 연산자 사용
        params.append("%" + name + "%")

    if production_year != ".":
        conditions.append("production_year = %s")
        params.append(production_year)

    if production_country != ".":
        conditions.append("production_country LIKE %s")  # 부분 일치 검색을 위해 LIKE 연산자 사용
        params.append("%" + production_country + "%")

    if genre != ".":
        conditions.append("genre_name LIKE %s")  # 부분 일치 검색을 위해 LIKE 연산자 사용
        params.append("%" + genre + "%")

    if director != ".":
        conditions.append("name LIKE %s")  # 부분 일치 검색을 위해 LIKE 연산자 사용
        params.append("%" + director + "%")

    # SQL 쿼리 생성
    sql = """
        SELECT m.name_kor, m.production_year, m.production_country, g.genre_name, d.name
        FROM Movie m
        JOIN Movie_Genre mg ON m.movie_id = mg.movie_id
        JOIN Genre g ON mg.genre_id = g.genre_id
        JOIN Movie_Director md ON m.movie_id = md.movie_id
        JOIN Director d ON md.director_id = d.director_id
    """
    if conditions:
        sql += "WHERE " + " AND ".join(conditions)

    # 영화 검색
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, tuple(params))
            results = cursor.fetchall()

            if results:
                print("\n[검색 결과]")
                for result in results:
                    print("영화 제목:", result[0])
                    print("제작 연도:", result[1])
                    print("제작 국가:", result[2])
                    print("장르:", result[3])
                    print("감독:", result[4])
                    print()
            else:
                print("검색 결과가 없습니다.")
    except Exception as e:
        print(f"영화 검색 중 오류가 발생했습니다: {str(e)}")

    print("검색이 완료되었습니다.")

# 영화 검색 실행
search_movies()

# 연결 종료
connection.close()