import duckdb

def init_db():
    con = duckdb.connect("nyc_taxi.duckdb")
    con.execute(open("./sql/00_init_duckdb.sql").read())
    return con