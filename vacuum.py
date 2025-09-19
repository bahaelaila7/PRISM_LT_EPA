import sqlite3
import argparse
from pathlib import Path

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='Vacuum Sqlite DB')
    parser.add_argument('db_file', type=Path)
    args = parser.parse_args()

    with sqlite3.connect(args.db_file) as conn:
        conn.execute('vacuum;')
