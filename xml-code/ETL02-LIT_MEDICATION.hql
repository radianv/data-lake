add JAR hdfs://localhost:8020/tmp/hivexmlserde-1.0.5.3.jar;

use ${hiveconf:MY_SCHEMA};

from
(select
reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',a.nombre_receta) as sid_md
,regexp_extract(a.INPUT__FILE__NAME, '(V|v)(.*[1-9])',2) as version_receta
,regexp_extract(a.nombre_receta, 'urn:uuid:(.*)', 1) as nombre_receta
,a.id_medicamento
,a.nombre_medicamento
,from_unixtime(unix_timestamp()) as fec_ini
from 
${hiveconf:MY_SCHEMA}.ebt_medication as a left outer join (select * from ${hiveconf:MY_SCHEMA}.iit_medication) j 
on regexp_extract(a.nombre_receta, 'urn:uuid:(.*)', 1) = j.nombre_receta and regexp_extract(a.INPUT__FILE__NAME, '(V|v)(.*[1-9])',2) = j.version_receta
where j.nombre_receta is null and j.version_receta is null
) i
insert into table ${hiveconf:MY_SCHEMA}.iit_medication
select i.sid_md
,i.version_receta
,i.nombre_receta
,i.id_medicamento
,i.nombre_medicamento
,i.fec_ini;

