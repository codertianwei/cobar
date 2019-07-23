import MySQLdb

def query(host, port, user, password, database, sql):
    try:
        conn=MySQLdb.connect(host=host, port=port, user=user, passwd=password, db=database)
        try:
            cur = conn.cursor(MySQLdb.cursors.DictCursor)
            cur.execute(sql)
            rows = cur.fetchall()
            conn.commit()
        except Exception as e:
            print(e)
        finally:
            if cur:
                cur.close()
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()

    return rows
