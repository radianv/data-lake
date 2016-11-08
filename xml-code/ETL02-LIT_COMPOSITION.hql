add JAR hdfs://localhost:8020/tmp/hivexmlserde-1.0.5.3.jar;

use ${hiveconf:MY_SCHEMA};

from
(select
reflect('org.apache.commons.codec.digest.DigestUtils'
,'sha256Hex'
,a.nombre_receta) as sid_md
,regexp_extract(INPUT__FILE__NAME, '(V|v)(.*[1-9])',2) as version_receta
,a.nombre_receta
,a.fecha_expedicion
,a.estatus_comp
,a.id_receta_medica
,a.paciente
,a.medico
,a.folio
,a.fecha_expedicionrec
,a.estatus
,a.tratamiento
,a.id_tipo_receta
,a.dx_receta
,from_unixtime(unix_timestamp()) as fec_ini
from 
${hiveconf:MY_SCHEMA}.ebt_composition as a left outer join (select * from ${hiveconf:MY_SCHEMA}.iit_composition) j 
on a.nombre_receta = j.nombre_receta and regexp_extract(a.INPUT__FILE__NAME, '(V|v)(.*[1-9])',2) = j.version_receta
where j.nombre_receta is null and j.version_receta is null
) i
insert into table ${hiveconf:MY_SCHEMA}.iit_composition
select i.sid_md
,i.version_receta
,i.nombre_receta
,i.fecha_expedicion
,i.estatus_comp
,i.id_receta_medica
,i.paciente
,i.medico
,i.folio
,i.fecha_expedicionrec
,i.estatus
,i.tratamiento
,i.id_tipo_receta
,i.dx_receta
,i.fec_ini;
