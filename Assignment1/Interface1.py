import psycopg2
from psycopg2 import sql

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    try:
        #Load the cursor to the open socket connection
        cur = openconnection.cursor()

        #create a temporary table to load the entire data along with timestamp to be dropped later
        cur.execute("create table tmp (userid integer, spc1 varchar, movieid integer, spc2 varchar, rating float, spc3 varchar, tmstmp integer)")

        # Using the filepath, open the file in read mode
        f = open(ratingsfilepath, 'r')

        # copy_from command to load the data into seperate columns: Note null columns are named columns "spc1, spc2, spc3" to handle the double '::'
        cur.copy_from(f, 'tmp', sep=':')

        # copy the requuired column to the ratings table using the given name and drop the temp table after
        cur.execute(sql.SQL("create table {} as select userid, movieid, rating from tmp").format(sql.Identifier(ratingstablename)))
        cur.execute('drop table tmp')

    finally:
        if cur:
            cur.close()

def rangePartition(ratingstablename, numberofpartitions, openconnection):
    try:
        # Load the cursor to the open socket connection
        cur = openconnection.cursor()

        # Create a metadata table and save the number of partiotions value to be used during insertion of new row
        cur.execute("create table rpart_metadata (numPartition integer)")
        cur.execute("INSERT INTO rpart_metadata (numPartition) VALUES (%s)",(numberofpartitions,))

        # get the range by dividing the max rating by num of partition
        rng = 5/numberofpartitions

        # iterate over each partition to find the assignments of the rows to partition based on their rating value
        for i in range(numberofpartitions):

            partition_name = 'range_part' + str(i)

            #find out the lower and upper bound of the ratings value for each partition
            frm_rng = rng*(i)
            to_rng = rng*(i+1)

            # for i = 0 the range should be inclusive of the lower bound value to handle edge cases
            if i==0:
                cur.execute(
                    sql.SQL("create table {} as "
                            "select * from {} "
                            "where rating>=%s and rating<=%s")
                        .format(sql.Identifier(partition_name),sql.Identifier(ratingstablename))
                    ,(frm_rng, to_rng))

            # or else just select the rows which satisfy the condition for_range<ratings<=to_range and place them into their corresponding parition
            else:
                cur.execute(
                    sql.SQL("create table {} as "
                            "select * from {} "
                            "where rating>%s and rating<=%s")
                        .format(sql.Identifier(partition_name), sql.Identifier(ratingstablename))
                    ,(frm_rng, to_rng))


    finally:
        if cur:
            cur.close()

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    try:
        cur = openconnection.cursor()

        #get the total number of rows in the table
        cur.execute(sql.SQL("select count(*) from {}").format(sql.Identifier(ratingstablename)))
        total_rows = cur.fetchone()[0]

        # find out the final partition in which the last row will get assigned to save it to the metadata table
        lst_token = (total_rows-1)%numberofpartitions

        # create metadata table and store information about the last r_rbin token and the number of partitions to be used later during insertion
        meta_tbl = 'rrbin_metadata'
        cur.execute(sql.SQL("create table {} (numPartition integer, token integer)").format(sql.Identifier(meta_tbl)))
        cur.execute(sql.SQL("INSERT INTO {} (numPartition, token) VALUES (%s, %s)").format(sql.Identifier(meta_tbl)),(numberofpartitions, lst_token))

        # for each partition find out the correct assignments and create a table with the correct partition number
        for i in range(numberofpartitions):
            partition_name = 'rrobin_part' + str(i)

            # select all rows whose (row_num-1) dicvided by num_of_partitions remainder equals to the token number
            cur.execute(
                sql.SQL("create table {} as "
                        "select userid, movieid, rating from "
                        "(select userid, movieid, rating, ROW_NUMBER() OVER() as rnum from {}) as t " #dummy table to have row_mber column
                        "where mod((t.rnum-1), %s) = %s")
                    .format(sql.Identifier(partition_name), sql.Identifier(ratingstablename)),
                (numberofpartitions, i))

    finally:
        if cur:
            cur.close()

def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    try:
        cur = openconnection.cursor()

        # first insert the required row in the main ratings table
        cur.execute(
            sql.SQL("INSERT INTO {} "
                    "(userid, movieid, rating) VALUES (%s, %s, %s)")
                .format(sql.Identifier(ratingstablename))
            ,(userid, itemid, rating))

        # read the metadata to find what was the last partition in the round robin wo have been assigned to and the number of partitions
        cur.execute("select * from rrbin_metadata")
        row = cur.fetchone()
        num_partitions = row[0]
        lst_token = row[1]

        # if the last token was the last partition reset next token to 0, otherwise assign the next partition as the token for insertion
        if(lst_token==num_partitions-1):
            nxt_token = 0
        else:
            nxt_token = lst_token+1

        # Use the next assignment token to find out the partition name that the insertion should be made to and insert the row there
        insrt_tbl_name = 'rrobin_part'+ str(nxt_token)
        cur.execute(
            sql.SQL("INSERT INTO {} "
                    "(userid, movieid, rating) VALUES (%s, %s, %s)")
                .format(sql.Identifier(insrt_tbl_name))
            ,(userid, itemid, rating))

        #Update the metadata table's token information
        cur.execute('UPDATE rrbin_metadata SET token = %s',(nxt_token,))

    finally:
        if cur:
            cur.close()


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    try:
        cur = openconnection.cursor()

        # first insert the required row in the main ratings table
        cur.execute(
            sql.SQL("INSERT INTO {} "
                    "(userid, movieid, rating) VALUES (%s, %s, %s)")
                .format(sql.Identifier(ratingstablename))
            , (userid, itemid, rating))

        # read the metadata to find the total number of partitions that were made to be able to calculate each range's bounds
        cur.execute("select * from rpart_metadata")
        num_partition = cur.fetchone()[0]

        # calculate the range using this information
        rng = 5 / num_partition

        # Check for edge cases to maintain completeness by checking if the remainder is 0
        if(rating%rng==0):

            # if rating = 0, first partition else put it to one partition below
            if(rating//rng == 0):
                assgn_partition = 0
            else:
                assgn_partition = (rating//rng) - 1
        else:
            assgn_partition = (rating // rng)
        insrt_tbl = 'range_part'+str(int(assgn_partition))

        # execute insertion into the calculated partition number
        cur.execute(
            sql.SQL("INSERT INTO {} "
                    "(userid, movieid, rating) VALUES (%s, %s, %s)")
                .format(sql.Identifier(insrt_tbl)),
            (userid, itemid, rating))

    finally:
        if cur:
            cur.close()

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
