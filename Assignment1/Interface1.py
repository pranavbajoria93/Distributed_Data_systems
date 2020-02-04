import psycopg2
from psycopg2 import sql

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    cur.execute("create table tmp (userid integer, spc1 varchar, movieid integer, spc2 varchar, rating float, spc3 varchar, tmstmp integer)")
    f = open(ratingsfilepath, 'r')
    cur.copy_from(f, 'tmp', sep=':')
    cur.execute(sql.SQL("create table {} as select userid, movieid, rating from tmp").format(sql.Identifier(ratingstablename)))
    cur.execute('drop table tmp')


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()

    cur.execute("create table rpart_metadata (numPartition integer)")
    cur.execute("INSERT INTO rpart_metadata (numPartition) VALUES (%s)",(numberofpartitions,))
    rng = 5/numberofpartitions
    for i in range(numberofpartitions):
        partition_name = 'range_part' + str(i)
        frm_rng = rng*(i)
        to_rng = rng*(i+1)
        # print('-------Range from ', frm_rng, ' to ', to_rng, ' partition name ----------', partition_name)
        if i==0:
            cur.execute(
                sql.SQL("create table {} as select * from {} where rating>=%s and rating<=%s").format(sql.Identifier(partition_name),sql.Identifier(ratingstablename)),(frm_rng, to_rng))
        else:
            cur.execute(
                sql.SQL("create table {} as select * from {} where rating>%s and rating<=%s").format(sql.Identifier(partition_name), sql.Identifier(ratingstablename)),(frm_rng, to_rng))

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    cur.execute(sql.SQL("select count(*) from {}").format(sql.Identifier(ratingstablename)))
    total_rows = cur.fetchone()[0]
    lst_token = (total_rows-1)%numberofpartitions
    meta_tbl = 'rrbin_metadata'
    cur.execute(sql.SQL("create table {} (numPartition integer, token integer)").format(sql.Identifier(meta_tbl)))
    cur.execute(sql.SQL("INSERT INTO {} (numPartition, token) VALUES (%s, %s)").format(sql.Identifier(meta_tbl)),(numberofpartitions, lst_token))

    for i in range(numberofpartitions):
        partition_name = 'rrobin_part' + str(i)
        cur.execute(
            sql.SQL("create table {} as "
                    "select userid, movieid, rating from "
                    "(select userid, movieid, rating, ROW_NUMBER() OVER() as rnum from {}) as t "
                    "where mod((t.rnum-1), %s) = %s").format(sql.Identifier(partition_name), sql.Identifier(ratingstablename)),
            (numberofpartitions, i))


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("select * from rrbin_metadata")
    row = cur.fetchone()
    num_partitions = row[0]
    lst_token = row[1]
    if(lst_token==num_partitions-1):
        nxt_token = 0
    else:
        nxt_token = lst_token+1
    insrt_tbl_name = 'rrobin_part'+ str(nxt_token)
    cur.execute(
        sql.SQL("INSERT INTO {} "
                "(userid, movieid, rating) VALUES (%s, %s, %s)").format(sql.Identifier(insrt_tbl_name)), (userid, itemid, rating))
    # print('Inserting into table ', insrt_tbl_name)
    cur.execute('UPDATE rrbin_metadata SET token = %s',(nxt_token,))
    cur.execute("select * from rrbin_metadata")

    # print('new token value updated from ', lst_token, 'to ', cur.fetchone()[1])


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("select * from rpart_metadata")
    num_partition = cur.fetchone()[0]
    rng = 5 / num_partition
    if(rating%rng==0):
        if(rating//rng == 0):
            assgn_partition = 0
        else:
            assgn_partition = (rating//rng) - 1
    else:
        assgn_partition = (rating // rng)
    insrt_tbl = 'range_part'+str(int(assgn_partition))
    cur.execute(
        sql.SQL("INSERT INTO {} "
                "(userid, movieid, rating) VALUES (%s, %s, %s)").format(sql.Identifier(insrt_tbl)),
        (userid, itemid, rating))

def createDB(dbname='dds_assignment1'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
