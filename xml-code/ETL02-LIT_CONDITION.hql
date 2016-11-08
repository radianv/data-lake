add JAR hdfs://localhost:8020/tmp/hivexmlserde-1.0.5.3.jar;

use ${hiveconf:MY_SCHEMA};

from
(select
reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',a.nombre_receta) as sid_md
,regexp_extract(INPUT__FILE__NAME, '(V|v)(.*[1-9])',2) as version_receta
,a.nombre_receta
,a.diagnostico_id
,a.diagnostico_id_nota
,a.diagnostico_nota
,a.uuid_diagnostico
,a.subject
,a.asserter
,a.id_diagnostico
,a.diagnostico
,a.status
,a.nombre_condition
,from_unixtime(unix_timestamp()) as fec_ini
from 
${hiveconf:MY_SCHEMA}.ebt_condition as a left outer join (select * from ${hiveconf:MY_SCHEMA}.iit_condition) j 
on a.nombre_receta = j.nombre_receta and regexp_extract(a.INPUT__FILE__NAME, '(V|v)(.*[1-9])',2) = j.version_receta
where j.nombre_receta is null and j.version_receta is null
) i
insert into table ${hiveconf:MY_SCHEMA}.iit_condition
select i.sid_md
,i.version_receta
,i.nombre_receta
,i.diagnostico_id
,i.diagnostico_id_nota
,i.diagnostico_nota
,i.uuid_diagnostico
,i.subject
,i.asserter
,i.id_diagnostico
,i.diagnostico
,i.status
,i.nombre_condition
,i.fec_ini;

