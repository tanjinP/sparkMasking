import psycopg2
import boto3
import yaml


bucket_folder = "s3://vts-dummyData/data/"

# loading yaml file
config=yaml.load(open('config.yml'))

# Instantiating boto to retrieve AWS credentials
session = boto3.Session()
credentials = session.get_credentials()

# Credentials are refreshable, so accessing your access key / secret key
# separately can lead to a race condition. Use this to get an actual matched
# set.
credentials = credentials.get_frozen_credentials()
access_key = credentials.access_key
secret_key = credentials.secret_key

# Connecting to redshift db
con=psycopg2.connect(dbname= 'dev', host=config['HOST'], port= '5439', user= config['USER'], password= config['PASS'])

tables=[config['TABLE1'], config['TABLE2']]

for table in tables:
    cur = con.cursor()

    # executing query to load data into redshift
    cur.execute("""copy %s from '%s%s' credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'
    delimiter ',' region 'us-east-1' maxerror as 250;""" % (table, bucket_folder, table, access_key, secret_key))

    cur.close()
    # commit necessary to enact query into redshift
    con.commit()

con.close()
