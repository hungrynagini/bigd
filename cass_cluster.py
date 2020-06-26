from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra9182')

cluster = Cluster(['172.31.72.233'], auth_provider=auth_provider)
