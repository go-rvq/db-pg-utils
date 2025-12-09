package backup

const (
	createSQL = `
CREATE OR REPLACE FUNCTION db_backup__create(message TEXT, serveraddr TEXT, query TEXT) RETURNS TABLE (created_at TIMESTAMPTZ, size BIGINT, message TEXT) AS $$
import subprocess
from datetime import datetime
from os import path, makedirs, remove
from http.client import HTTPConnection
from urllib.parse import urlencode, parse_qs

h = HTTPConnection(serveraddr)

dbname = plpy.execute("SELECT current_database() as v")[0]['v']
now = datetime.utcnow().replace(microsecond=0)
basename = dbname+"-"+now.strftime("%Y%m%d-%H%M%S-%f.pg")
filename = path.join("/tmp", basename)

p = subprocess.run(['pg_dump', '-xO', '-F', 'c', '-f', filename, dbname], capture_output=True)
if p.returncode:
	plpy.error(p.stderr.decode())
else:
	size = path.getsize(filename)
	with open(filename, "rb") as f:
		q = dict(
			name=basename, 
			db=dbname, 
			ts=str(int(now.timestamp())),
			size=size, 
			message=message
		)

		if query:
			qd = parse_qs(query)
			q.update(qd)

		h.request("POST", "/?"+urlencode(q), headers={"Host": serveraddr}, body=f, encode_chunked=True)
		response = h.getresponse()
		ret = f"{response.status} {response.reason}: {response.read()}"
		remove(filename)
		if response.status != 200:
			plpy.error(ret)
		else:
			plpy.info(ret)
	yield(now, size, message)
$$ LANGUAGE plpython3u;`
)
