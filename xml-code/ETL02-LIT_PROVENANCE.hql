add JAR hdfs://localhost:8020/tmp/hivexmlserde-1.0.5.3.jar;

use ${hiveconf:MY_SCHEMA};

from
(select
reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',a.nombre_receta) as sid_md
,regexp_extract(INPUT__FILE__NAME, '(V|v)(.*[1-9])',2) as version_receta
,a.nombre_receta
,a.fec_atencion
,from_unixtime(unix_timestamp()) as fec_ini
from 
${hiveconf:MY_SCHEMA}.ebt_provenance as a left outer join (select * from ${hiveconf:MY_SCHEMA}.iit_provenance) j 
on a.nombre_receta = j.nombre_receta and regexp_extract(a.INPUT__FILE__NAME, '(V|v)(.*[1-9])',2) = j.version_receta
where j.nombre_receta is null and j.version_receta is null
) i
insert into table ${hiveconf:MY_SCHEMA}.iit_provenance
select i.sid_md
,i.version_receta
,i.nombre_receta
,i.fec_atencion
,i.fec_ini;

