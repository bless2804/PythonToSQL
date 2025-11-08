import os
import argparse
import psycopg2
from psycopg2.extras import RealDictCursor

# db settings (use your .env or defaults)
DB_HOST = os.getenv("PGHOST", "127.0.0.1")
DB_PORT = int(os.getenv("PGPORT", "5432"))
DB_NAME = os.getenv("PGDATABASE", "a3students_db")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASS = os.getenv("PGPASSWORD", "student")

# open a connection
def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )


# gets students
def get_all_students():
    sql = """
        SELECT student_id, first_name, last_name, email, enrollment_date
        FROM students
        ORDER BY student_id;
    """
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    if not rows:
        print("(no rows)")
    else:
        for r in rows:
            print(dict(r))


# adds student
def add_student(first_name, last_name, email, enrollment_date):
    sql = """
        INSERT INTO students (first_name, last_name, email, enrollment_date)
        VALUES (%s, %s, %s, %s)
        RETURNING student_id;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (first_name, last_name, email, enrollment_date))
        new_id = cur.fetchone()[0]
        conn.commit()
    print(f"ok: inserted student_id={new_id}")



# updates student 
def update_student_email(student_id, new_email):
    sql = "UPDATE students SET email = %s WHERE student_id = %s;"
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (new_email, student_id))
        conn.commit()
        count = cur.rowcount
    print(f"ok: updated rows={count}")



#deletes students 
def delete_student(student_id):
    sql = "DELETE FROM students WHERE student_id = %s;"
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (student_id,))
        conn.commit()
        count = cur.rowcount
    print(f"ok: deleted rows={count}")


def parse_args():
    parser = argparse.ArgumentParser(description="A3: Students table CRUD CLI")
    subparsers = parser.add_subparsers(dest="operation", required=True)

    # init: create table and seed rows
    subparsers.add_parser(
        "init",
        help="Create the students table and insert 3 seed rows"
    )

    # get: list all students
    subparsers.add_parser(
        "get",
        help="List all students ordered by student_id"
    )

    # add: insert one student
    add_parser = subparsers.add_parser(
        "add",
        help="Add a new student"
    )
    add_parser.add_argument("--first", dest="first_name", required=True, help="First name")
    add_parser.add_argument("--last", dest="last_name", required=True, help="Last name")
    add_parser.add_argument("--email", dest="email", required=True, help="Email (must be unique)")
    add_parser.add_argument("--date", dest="enrollment_date", default=None, help="Enrollment date YYYY-MM-DD (optional)")

    # update: change a student's email
    update_parser = subparsers.add_parser(
        "update",
        help="Update a student's email by student_id"
    )
    update_parser.add_argument("--id", dest="student_id", type=int, required=True, help="Student ID")
    update_parser.add_argument("--email", dest="new_email", required=True, help="New email")

    # delete: remove a student
    delete_parser = subparsers.add_parser(
        "delete",
        help="Delete a student by student_id"
    )
    delete_parser.add_argument("--id", dest="student_id", type=int, required=True, help="Student ID")

    return parser.parse_args()



def main():
    args = parse_args()

    match args.operation:
        case "delete":
            delete_student(args.student_id)   
        case "update":
            update_student_email(args.student_id, args.new_email)
        case "add":
            add_student(args.first_name, args.last_name, args.email, args.enrollment_date)
        case "get":
            get_all_students()
        case _:
            print("ERROR! Use: init | get | add | update | delete")


if __name__ == "__main__":
    main()
