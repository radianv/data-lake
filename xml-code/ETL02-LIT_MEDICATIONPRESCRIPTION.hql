add JAR hdfs://localhost:8020/tmp/hivexmlserde-1.0.5.3.jar;

use ${hiveconf:MY_SCHEMA};

from
(select
reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',a.nombre_receta) as sid_md
,regexp_extract(a.INPUT__FILE__NAME, '(V|v)(.*[1-9])',2) as version_receta
,regexp_extract(a.nombre_receta, 'urn:uuid:(.*)', 1) as nombre_receta
,a.status_medicamento
,a.uuid_med_presc
,a.fecha_prescripcion
,a.status_med_presc
,a.paciente
,a.prescriber
,a.medicamento
,a.dosis_instruccion
,a.dosis_repeticion_frecuencia
,a.dosis_repeticion_duracion
,a.dosis_repeticion_unidad
,a.dosis_repeticion_diastratamiento
,a.administracion_descripcion
,a.administracion
,a.administracion_codigo
,a.dosis_cantidad
,a.dosis_codigo
,a.descripcion_medicamento
,a.surtimiento_cantidad
,a.surtimiento_unidad
,a.surtimiento_unidad_codigo
,from_unixtime(unix_timestamp()) as fec_ini
from 
${hiveconf:MY_SCHEMA}.ebt_medicationprescription as a left outer join (select * from ${hiveconf:MY_SCHEMA}.iit_medicationprescription) j 
on regexp_extract(a.nombre_receta, 'urn:uuid:(.*)', 1) = j.nombre_receta and regexp_extract(a.INPUT__FILE__NAME, '(V|v)(.*[1-9])',2) = j.version_receta
where j.nombre_receta is null and j.version_receta is null
) i
insert into table ${hiveconf:MY_SCHEMA}.iit_medicationprescription
select i.sid_md
,i.version_receta
,i.nombre_receta
,i.status_medicamento
,i.uuid_med_presc
,i.fecha_prescripcion
,i.status_med_presc
,i.paciente
,i.prescriber
,i.medicamento
,i.dosis_instruccion
,i.dosis_repeticion_frecuencia
,i.dosis_repeticion_duracion
,i.dosis_repeticion_unidad
,i.dosis_repeticion_diastratamiento
,i.administracion_descripcion
,i.administracion
,i.administracion_codigo
,i.dosis_cantidad
,i.dosis_codigo
,i.descripcion_medicamento
,i.surtimiento_cantidad
,i.surtimiento_unidad
,i.surtimiento_unidad_codigo
,i.fec_ini;


