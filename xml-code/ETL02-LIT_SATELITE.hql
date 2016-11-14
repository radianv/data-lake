add JAR hdfs://localhost:8020/tmp/hivexmlserde-1.0.5.3.jar;

use ${hiveconf:MY_SCHEMA};

from
(select
reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',a.nombre_receta) as sid_md
,regexp_extract(a.INPUT__FILE__NAME, '(V|v)(.*[1-9])',2) as version_receta
,regexp_extract(a.nombre_receta, 'urn:uuid:(.*)', 1) as nombre_receta
,a.id_delegacion
,a.delegacion_desc
,a.fec_condition
,a.diagnostico
,a.id_diagnostico
,a.estatus_receta
,a.administracion
,a.administracion_codigo
,a.administracion_descripcion
,a.fecha_prescripcion
,a.dosis_cantidad
,a.dosis_codigo
,a.dosis_repeticion_duracion
,a.dosis_repeticion_frecuencia
,a.dosis_repeticion_unidad
,a.surtimiento_cantidad
,a.surtimiento_unidad
,a.surtimiento_unidad_codigo
,a.id_medicamento
,a.nombre_medicamento
,a.medico
,a.cedula
,a.matricula
,a.nss
,a.curp
,a.paciente
,a.agregado_afiliacion
,a.genero
,a.agregado_medico
,a.fecha_nacimiento
,a.tipo_generacion
,a.id_tipo_generacion
,a.id_tipo_receta
,a.dx_receta
,a.id_umf
,a.umf_desc
,a.num_consultorio
,from_unixtime(unix_timestamp()) as fec_ini
from 
${hiveconf:MY_SCHEMA}.ebt_satelite as a left outer join (select * from ${hiveconf:MY_SCHEMA}.iit_satelite) j 
on regexp_extract(a.nombre_receta, 'urn:uuid:(.*)', 1) = j.nombre_receta and regexp_extract(a.INPUT__FILE__NAME, '(V|v)(.*[1-9])',2) = j.version_receta
where j.nombre_receta is null and j.version_receta is null
) i
insert into table ${hiveconf:MY_SCHEMA}.iit_satelite
select i.sid_md
,i.version_receta
,i.nombre_receta
,i.id_delegacion
,i.delegacion_desc
,i.fec_condition
,i.diagnostico
,i.id_diagnostico
,i.estatus_receta
,i.administracion
,i.administracion_codigo
,i.administracion_descripcion
,i.fecha_prescripcion
,i.dosis_cantidad
,i.dosis_codigo
,i.dosis_repeticion_duracion
,i.dosis_repeticion_frecuencia
,i.dosis_repeticion_unidad
,i.surtimiento_cantidad
,i.surtimiento_unidad
,i.surtimiento_unidad_codigo
,i.id_medicamento
,i.nombre_medicamento
,i.medico
,i.cedula
,i.matricula
,i.nss
,i.curp
,i.paciente
,i.agregado_afiliacion
,i.genero
,i.agregado_medico
,i.fecha_nacimiento
,i.tipo_generacion
,i.id_tipo_generacion
,i.id_tipo_receta
,i.dx_receta
,i.id_umf
,i.umf_desc
,i.num_consultorio
,i.fec_ini;



