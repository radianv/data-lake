add JAR hdfs://localhost:8020/tmp/hivexmlserde-1.0.5.3.jar;

use ${hiveconf:MY_SCHEMA};
DROP TABLE IF EXISTS ${hiveconf:MY_SCHEMA}.ebt_composition;
DROP TABLE IF EXISTS ${hiveconf:MY_SCHEMA}.ebt_condition;
DROP TABLE IF EXISTS ${hiveconf:MY_SCHEMA}.ebt_medicationprescription;
DROP TABLE IF EXISTS ${hiveconf:MY_SCHEMA}.ebt_medico;
DROP TABLE IF EXISTS ${hiveconf:MY_SCHEMA}.ebt_paciente;
DROP TABLE IF EXISTS ${hiveconf:MY_SCHEMA}.ebt_provenance;
DROP TABLE IF EXISTS ${hiveconf:MY_SCHEMA}.ebt_medication;
DROP TABLE IF EXISTS ${hiveconf:MY_SCHEMA}.ebt_satelite;

DROP TABLE IF EXISTS ${hiveconf:MY_SCHEMA}.iit_composition;
DROP TABLE IF EXISTS ${hiveconf:MY_SCHEMA}.iit_condition;
DROP TABLE IF EXISTS ${hiveconf:MY_SCHEMA}.iit_medicationprescription;
DROP TABLE IF EXISTS ${hiveconf:MY_SCHEMA}.iit_medico;
DROP TABLE IF EXISTS ${hiveconf:MY_SCHEMA}.iit_paciente;
DROP TABLE IF EXISTS ${hiveconf:MY_SCHEMA}.iit_provenance;
DROP TABLE IF EXISTS ${hiveconf:MY_SCHEMA}.iit_medication;
DROP TABLE IF EXISTS ${hiveconf:MY_SCHEMA}.iit_satelite;


CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:MY_SCHEMA}.ebt_composition(nombre_receta string, fecha_expedicion string, estatus_comp string, id_receta_medica string, paciente string, medico string, folio string, fecha_expedicionrec string, estatus string, tratamiento string, id_tipo_receta string, dx_receta string)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.nombre_receta"="//Composition/identifier[label[@value='UUID']]/value/@value",
"column.xpath.fecha_expedicion"="//Composition/date/@value",
"column.xpath.estatus_comp"="//Composition/status/@value",
"column.xpath.id_receta_medica"="//Composition/identifier[label[value='UUID']]/value/@value",
"column.xpath.paciente"="//Composition/subject/display/@value",
"column.xpath.medico"="//Composition/author/display/@value",
"column.xpath.folio"="//extension[@url='http://imss.gob.mx/hie/hl7/fhir/extensions#receta-folio']/valueString/@value",
"column.xpath.fecha_expedicionrec"="//extension[@url='http://imss.gob.mx/hie/hl7/fhir/extensions#receta-fechaExpedicion']/valueString/@value",
"column.xpath.estatus"="//extension[@url='http://imss.gob.mx/hie/hl7/fhir/extensions#receta-estatus']/valueString/@value",
"column.xpath.tratamiento"="//extension[@url='http://imss.gob.mx/hie/hl7/fhir/extensions#receta-tratamiento']/valueString/@value",
"column.xpath.id_tipo_receta"="//extension[@url='http://imss.gob.mx/hie/hl7/fhir/extensions#receta-tipo']/valueCodableConcept/coding/code/@value",
"column.xpath.dx_receta"="//extension[@url='http://imss.gob.mx/hie/hl7/fhir/extensions#receta-tipo']/valueCodableConcept/coding/display/@value"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION "${hiveconf:MY_EBT_LOCATION}"
TBLPROPERTIES (
"xmlinput.start"="<feed ",
"xmlinput.end"="</feed>"
);

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:MY_SCHEMA}.ebt_condition(nombre_receta string, diagnostico_id string, diagnostico_id_nota string, diagnostico_nota string, uuid_diagnostico string, subject Array<string>, asserter Array<string>, id_diagnostico Array<string>, diagnostico Array<string>, status Array<string>, nombre_condition Array<string>)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.nombre_receta"="//Composition/identifier[label[@value='UUID']]/value/@value",
"column.xpath.diagnostico_id"="//extension[@url='http://imss.gob.mx/hie/hl7/fhir/extensions#rdiagnostico-id']/valueString/@value",
"column.xpath.diagnostico_id_nota"="//extension[@url='http://imss.gob.mx/hie/hl7/fhir/extensions#rdiagnostico-id-nota']/valueString/@value",
"column.xpath.diagnostico_nota"="//extension[@url='http://imss.gob.mx/hie/hl7/fhir/extensions#rdiagnostico-nota']/valueString/@value",
"column.xpath.uuid_diagnostico"="//Condition/identifier[@label='CVE_ATENCION']/value/@value",
"column.xpath.subject"="//Condition/subject/display/@value",
"column.xpath.asserter"="//Condition/asserter/display/@value",
"column.xpath.id_diagnostico"="//Condition/code/coding/code/@value",
"column.xpath.diagnostico"="//Condition/code/text/@value",
"column.xpath.status"="//Condition/status/@value",
"column.xpath.nombre_condition"="//Condition/category/coding/display/@value"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION "${hiveconf:MY_EBT_LOCATION}"
TBLPROPERTIES (
"xmlinput.start"="<feed ",
"xmlinput.end"="</feed>"
);

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:MY_SCHEMA}.ebt_medicationprescription(nombre_receta string, status_medicamento Array<string>, uuid_med_presc string, fecha_prescripcion Array<string>, status_med_presc Array<string>, paciente Array<string>, prescriber Array<string>, medicamento Array<string>, dosis_instruccion Array<string>, dosis_repeticion_frecuencia string, dosis_repeticion_duracion string, dosis_repeticion_unidad string, dosis_repeticion_diastratamiento string, administracion_descripcion Array<string>, administracion Array<string>, administracion_codigo Array<string>, dosis_cantidad Array<string>, dosis_codigo Array<string>, descripcion_medicamento Array<string>, surtimiento_cantidad Array<string>, surtimiento_unidad Array<string>, surtimiento_unidad_codigo Array<string>)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.nombre_receta"="//Composition/identifier[label[@value='UUID']]/value/@value",
"column.xpath.status_medicamento"="//extension[@url='http://imss.gob.mx/hie/hl7/fhir/extensions#receta-medicamento-estatus']/valueString/@value",
"column.xpath.uuid_med_presc"="//MedicationPrescription/identifier[label[value='UUID']]/value/@value",
"column.xpath.fecha_prescripcion"="//MedicationPrescription/dateWritten/@value",
"column.xpath.status_med_presc"="//MedicationPrescription/status/@value",
"column.xpath.paciente"="//MedicationPrescription/patient/display/@value",
"column.xpath.prescriber"="//MedicationPrescription/prescriber/display/@value",
"column.xpath.medicamento"="//MedicationPrescription/medication/display/@value",
"column.xpath.dosis_instruccion"="//MedicationPrescription/dosageInstruction/text/@value",
"column.xpath.dosis_repeticion_frecuencia"="//MedicationPrescription/dosageInstruction/repeat/frequency/@value",
"column.xpath.dosis_repeticion_duracion"="//MedicationPrescription/dosageInstruction/repeat/duration/@value",
"column.xpath.dosis_repeticion_unidad"="//MedicationPrescription/dosageInstruction/repeat/units/@value",
"column.xpath.dosis_repeticion_diastratamiento"="//MedicationPrescription/dosageInstruction/repeat/count/@value",
"column.xpath.administracion_descripcion"="//MedicationPrescription/dosageInstruction/route/coding/display/@value",
"column.xpath.administracion"="//MedicationPrescription/dosageInstruction/route/text/@value",
"column.xpath.administracion_codigo"="//MedicationPrescription/dosageInstruction/route/coding/code/@value",
"column.xpath.dosis_cantidad"="//MedicationPrescription/dosageInstruction/doseQuantity/value/@value",
"column.xpath.dosis_codigo"="//MedicationPrescription/dosageInstruction/doseQuantity/code/@value",
"column.xpath.descripcion_medicamento"="//MedicationPrescription/dosageInstruction/doseQuantity/units/@value",
"column.xpath.surtimiento_cantidad"="//MedicationPrescription/dispense/quantity/value/@value",
"column.xpath.surtimiento_unidad"="//MedicationPrescription/dispense/quantity/units/@value",
"column.xpath.surtimiento_unidad_codigo"="//MedicationPrescription/dispense/quantity/code/@value"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION "${hiveconf:MY_EBT_LOCATION}"
TBLPROPERTIES (
"xmlinput.start"="<feed ",
"xmlinput.end"="</feed>"
);

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:MY_SCHEMA}.ebt_medico(nombre_receta string, matricula string, cedula string, medico string)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.nombre_receta"="//Composition/identifier[label[@value='UUID']]/value/@value",
"column.xpath.matricula"="//Practitioner/identifier[label[@value=MATRICULA']]/value/@value",
"column.xpath.cedula"="//Practitioner/identifier[label[@value=CEDULA']]/value/@value",
"column.xpath.medico"="//Practitioner/name/text/@value"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION "${hiveconf:MY_EBT_LOCATION}"
TBLPROPERTIES (
"xmlinput.start"="<feed ",
"xmlinput.end"="</feed>"
);

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:MY_SCHEMA}.ebt_paciente(nombre_receta string, cxcurp string, curp string, nss string, agregado_medico string, agregado_afiliacion string, paciente string, apellidos string, nombre_pila string, genero string, fecha_nacimiento string)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.nombre_receta"="//Composition/identifier[label[@value='UUID']]/value/@value",
"column.xpath.cxcurp"="//identifier[label[@value='CX_CURP']]/value/@value",
"column.xpath.curp"="//Patient/identifier[label[@value='CURP']]/value/@value",
"column.xpath.nss"="//Patient/identifier[label[@value='NSS']]/value/@value",
"column.xpath.agregado_medico"="//Patient/identifier[label[@value='AGREGADO_MEDICO']]/value/@value",
"column.xpath.agregado_afiliacion"="//Patient/identifier[label[@value='AGREGADO_AFILIACION']]/value/@value",
"column.xpath.paciente"="//Patient/name/text/@value",
"column.xpath.apellidos"="//Patient/name/family/@value",
"column.xpath.nombre_pila"="//Patient/name/given/@value",
"column.xpath.genero"="//Patient/gender/coding/code/@value",
"column.xpath.fecha_nacimiento"="//Patient/birthDate/@value"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION "${hiveconf:MY_EBT_LOCATION}"
TBLPROPERTIES (
"xmlinput.start"="<feed ",
"xmlinput.end"="</feed>"
);

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:MY_SCHEMA}.ebt_provenance(nombre_receta string, fec_atencion string)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.nombre_receta"="//Composition/identifier[label[@value='UUID']]/value/@value",
"column.xpath.fec_atencion"="//Provenance/recorded/@value"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION "${hiveconf:MY_EBT_LOCATION}"
TBLPROPERTIES (
"xmlinput.start"="<feed ",
"xmlinput.end"="</feed>"
);

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:MY_SCHEMA}.ebt_medication(nombre_receta string, id_medicamento Array<string>, nombre_medicamento Array<string>)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.nombre_receta"="//Composition/identifier[label[@value='UUID']]/value/@value",
"column.xpath.id_medicamento"="//extension[@url='http://imss.gob.mx/hie/hl7/fhir/extensions#receta-medicamento-clave']/valueString/@value",
"column.xpath.nombre_medicamento"="//Medication/name/@value"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION "${hiveconf:MY_EBT_LOCATION}"
TBLPROPERTIES (
"xmlinput.start"="<feed ",
"xmlinput.end"="</feed>"
);


CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:MY_SCHEMA}.ebt_satelite(nombre_receta string, id_delegacion string, delegacion_desc string, fec_condition Array<string>, diagnostico Array<string>, id_diagnostico Array<string>, estatus_receta string, administracion Array<string>, administracion_codigo Array<string>, administracion_descripcion Array<string>, fecha_prescripcion Array<string>, dosis_cantidad Array<string>, dosis_codigo Array<string>, dosis_repeticion_duracion string, dosis_repeticion_frecuencia string, dosis_repeticion_unidad string, surtimiento_cantidad Array<string>, surtimiento_unidad Array<string>, surtimiento_unidad_codigo Array<string>, id_medicamento Array<string>, nombre_medicamento string, medico string, cedula string, matricula string, nss string, curp string, paciente string, agregado_afiliacion string, genero string, agregado_medico string, fecha_nacimiento string, tipo_generacion string, id_tipo_generacion string, id_tipo_receta string, dx_receta string, id_umf string, umf_desc string, num_consultorio string)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.nombre_receta"="//Composition/identifier[label[@value='UUID']]/value/@value",
"column.xpath.id_delegacion"="//Location/identifier[label[@value='CVE_ZONA']]/value/@value",
"column.xpath.delegacion_desc"="//Location[/identifier/label[@value='CVE_ZONA']]/name/@value",
"column.xpath.fec_condition"="//Condition/onsetDate/@value",
"column.xpath.diagnostico"="//Condition/code/text/@value",
"column.xpath.id_diagnostico"="//Condition/code/coding/code/@value",
"column.xpath.estatus_receta"="//extension[@url='http://imss.gob.mx/hie/hl7/fhir/extensions#receta-estatus']/valueString/@value",
"column.xpath.administracion"="//MedicationPrescription/dosageInstruction/route/text/@value",
"column.xpath.administracion_codigo"="//MedicationPrescription/dosageInstruction/route/coding/code/@value",
"column.xpath.administracion_descripcion"="//MedicationPrescription/dosageInstruction/route/coding/display/@value",
"column.xpath.fecha_prescripcion"="//MedicationPrescription/dateWritten/@value",
"column.xpath.dosis_cantidad"="//MedicationPrescription/dosageInstruction/doseQuantity/value/@value",
"column.xpath.dosis_codigo"="//MedicationPrescription/dosageInstruction/doseQuantity/code/@value",
"column.xpath.dosis_repeticion_duracion"="//MedicationPrescription/dosageInstruction/repeat/duration/@value",
"column.xpath.dosis_repeticion_frecuencia"="//MedicationPrescription/dosageInstruction/repeat/frequency/@value",
"column.xpath.dosis_repeticion_unidad"="//MedicationPrescription/dosageInstruction/repeat/units/@value",
"column.xpath.surtimiento_cantidad"="//MedicationPrescription/dispense/quantity/value/@value",
"column.xpath.surtimiento_unidad"="//MedicationPrescription/dispense/quantity/units/@value",
"column.xpath.surtimiento_unidad_codigo"="//MedicationPrescription/dispense/quantity/code/@value",
"column.xpath.id_medicamento"="//extension[@url='http://imss.gob.mx/hie/hl7/fhir/extensions#receta-medicamento-clave']/valueString/@value",
"column.xpath.nombre_medicamento"="//Medication/name/@value",
"column.xpath.medico"="//Practitioner/name/text/@value",
"column.xpath.cedula"="//Practitioner/identifier[label[@value='CEDULA']]/value/@value",
"column.xpath.matricula"="//Practitioner/identifier[label[@value='MATRICULA']]/value/@value",
"column.xpath.nss"="//Patient/identifier[label[@value='NSS']]/value/@value",
"column.xpath.curp"="//Patient/identifier[label[@value='CURP']]/value/@value",
"column.xpath.paciente"="//Patient/name/text/@value",
"column.xpath.agregado_afiliacion"="//Patient/identifier[label[@value='AGREGADO_AFILIACION']]/value/@value",
"column.xpath.genero"="//Patient/gender/coding/code/@value",
"column.xpath.agregado_medico"="//Patient/identifier[label[@value='AGREGADO_MEDICO']]/value/@value",
"column.xpath.fecha_nacimiento"="//Patient/birthDate/@value",
"column.xpath.tipo_generacion"="//extension[@url='http://imss.gob.mx/hie/hl7/fhir/extensions#receta-origen']/valueCodeableConcept/coding/display/@value",
"column.xpath.id_tipo_generacion"="//extension[@url='http://imss.gob.mx/hie/hl7/fhir/extensions#receta-origen']/valueCodeableConcept/coding/code/@value",
"column.xpath.id_tipo_receta"="//extension[@url='http://imss.gob.mx/hie/hl7/fhir/extensions#receta-tipo']/valueCodeableConcept/coding/code/@value",
"column.xpath.dx_receta"="//extension[@url='http://imss.gob.mx/hie/hl7/fhir/extensions#receta-tipo']/valueCodeableConcept/coding/display/@value",
"column.xpath.id_umf"="//Location/identifier[label[@value='CVE_PRESUPUESTAL']]/value/@value",
"column.xpath.umf_desc"="//Location[/identifier/label[@value='CVE_PRESUPUESTAL']]/name/@value",
"column.xpath.num_consultorio"="//extension[@url='http://imss.gob.mx/hie/hl7/fhir/extensions#paciente-consultorio']/valueString/@value"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION "${hiveconf:MY_EBT_LOCATION}"
TBLPROPERTIES (
"xmlinput.start"="<feed ",
"xmlinput.end"="</feed>"
);


CREATE TABLE IF NOT EXISTS ${hiveconf:MY_SCHEMA}.iit_composition (sid_md string, version_receta string, nombre_receta string, fecha_expedicion string, estatus_comp string, id_receta_medica string, paciente string, medico string, folio string, fecha_expedicionrec string, estatus string, tratamiento string, id_tipo_receta string, dx_receta string, fec_ini string) 
COMMENT 'Tabla composition'
TBLPROPERTIES ('creator'='ADVP', 'created_at'='2016-11-01');


CREATE TABLE IF NOT EXISTS ${hiveconf:MY_SCHEMA}.iit_condition(sid_md string, version_receta string, nombre_receta string, diagnostico_id string, diagnostico_id_nota string, diagnostico_nota string, uuid_diagnostico string, subject Array<string>, asserter Array<string>, id_diagnostico Array<string>, diagnostico Array<string>, status Array<string>, nombre_condition Array<string>, fec_ini string)
COMMENT 'Tabla Condition'
TBLPROPERTIES ('creator'='ADVP', 'created_at'='2016-11-01');


CREATE TABLE IF NOT EXISTS ${hiveconf:MY_SCHEMA}.iit_medicationprescription(sid_md string, version_receta string, nombre_receta string, status_medicamento Array<string>, uuid_med_presc string, fecha_prescripcion Array<string>, status_med_presc Array<string>, paciente Array<string>, prescriber Array<string>, medicamento Array<string>, dosis_instruccion Array<string>, dosis_repeticion_frecuencia string, dosis_repeticion_duracion string, dosis_repeticion_unidad string, dosis_repeticion_diastratamiento string, administracion_descripcion Array<string>, administracion Array<string>, administracion_codigo Array<string>, dosis_cantidad Array<string>, dosis_codigo Array<string>, descripcion_medicamento Array<string>, surtimiento_cantidad Array<string>, surtimiento_unidad Array<string>, surtimiento_unidad_codigo Array<string>, fec_ini string)
COMMENT 'Tabla Medication Prescription'
TBLPROPERTIES ('creator'='ADVP', 'created_at'='2016-11-01');


CREATE TABLE IF NOT EXISTS ${hiveconf:MY_SCHEMA}.iit_medico(sid_md string, version_receta string, nombre_receta string, matricula string, cedula string, medico string, fec_ini string)
COMMENT 'Tabla Medico'
TBLPROPERTIES ('creator'='ADVP', 'created_at'='2016-11-01');


CREATE TABLE IF NOT EXISTS ${hiveconf:MY_SCHEMA}.iit_paciente(sid_md string, version_receta string, nombre_receta string, cxcurp string, curp string, nss string, agregado_medico string, agregado_afiliacion string, paciente string, apellidos string, nombre_pila string, genero string, fecha_nacimiento string, fec_ini string)
COMMENT 'Tabla Paciente'
TBLPROPERTIES ('creator'='ADVP', 'created_at'='2016-11-01');


CREATE TABLE IF NOT EXISTS ${hiveconf:MY_SCHEMA}.iit_provenance(sid_md string, version_receta string, nombre_receta string, fec_atencion string, fec_ini string)
COMMENT 'Tabla Provenance'
TBLPROPERTIES ('creator'='ADVP', 'created_at'='2016-11-01');


CREATE TABLE IF NOT EXISTS ${hiveconf:MY_SCHEMA}.iit_medication(sid_md string, version_receta string, nombre_receta string, id_medicamento Array<string>, nombre_medicamento Array<string>, fec_ini string)
COMMENT 'Tabla Medication'
TBLPROPERTIES ('creator'='ADVP', 'created_at'='2016-11-01');


CREATE TABLE IF NOT EXISTS ${hiveconf:MY_SCHEMA}.iit_satelite(sid_md string, version_receta string, nombre_receta string, id_delegacion string, delegacion_desc string, fec_condition Array<string>, diagnostico Array<string>, id_diagnostico Array<string>, estatus_receta string, administracion Array<string>, administracion_codigo Array<string>, administracion_descripcion Array<string>, fecha_prescripcion Array<string>, dosis_cantidad Array<string>, dosis_codigo Array<string>, dosis_repeticion_duracion string, dosis_repeticion_frecuencia string, dosis_repeticion_unidad string, surtimiento_cantidad Array<string>, surtimiento_unidad Array<string>, surtimiento_unidad_codigo Array<string>, id_medicamento Array<string>, nombre_medicamento string, medico string, cedula string, matricula string, nss string, curp string, paciente string, agregado_afiliacion string, genero string, agregado_medico string, fecha_nacimiento string, tipo_generacion string, id_tipo_generacion string, id_tipo_receta string, dx_receta string, id_umf string, umf_desc string, num_consultorio string, fec_ini string)
COMMENT 'Tabla Satelite'
TBLPROPERTIES ('creator'='ADVP', 'created_at'='2016-11-01');

add JAR hdfs://localhost:8020/tmp/brickhouse-0.6.0.jar;
CREATE TEMPORARY FUNCTION array_index AS 'brickhouse.udf.collect.ArrayIndexUDF';
CREATE TEMPORARY FUNCTION numeric_range AS 'brickhouse.udf.collect.NumericRange';


DROP VIEW IF EXISTS ${hiveconf:MY_SCHEMA}.rvt_reporting;
CREATE VIEW ${hiveconf:MY_SCHEMA}.rvt_reporting AS
SELECT s.* FROM
(select a.sid_md
,a.version_receta
,a.nombre_receta
,a.fecha_expedicion
,a.id_receta_medica
,a.estatus
,b.id_medicamento
,b.nombre_medicamento
,c.status_medicamento
,c.medicamento
,c.surtimiento_cantidad
,c.descripcion_medicamento
,d.genero
,d.fecha_nacimiento
,d.nss
,e.delegacion_desc
,e.diagnostico
,e.umf_desc
,f.fec_atencion
from
${hiveconf:MY_SCHEMA}.iit_composition a
JOIN (
select
sid_md,
version_receta,
array_index( t.id_medicamento, n ) as  id_medicamento,
array_index( t.nombre_medicamento, n ) as  nombre_medicamento
from ( select sid_md, version_receta, id_medicamento, nombre_medicamento from ${hiveconf:MY_SCHEMA}.iit_medication ) t
lateral view numeric_range( size( nombre_medicamento )) n1 as n) as b on a.sid_md=b.sid_md and a.version_receta=b.version_receta
JOIN (
select
sid_md,
version_receta,
array_index( t.status_medicamento, n ) as  status_medicamento,
array_index( t.medicamento, n ) as  medicamento,
array_index( t.surtimiento_cantidad, n ) as  surtimiento_cantidad,
array_index( t.descripcion_medicamento, n ) as  descripcion_medicamento
from ( select sid_md, version_receta, status_medicamento, medicamento, surtimiento_cantidad, descripcion_medicamento from ${hiveconf:MY_SCHEMA}.iit_medicationprescription ) t
lateral view numeric_range( size( descripcion_medicamento )) n1 as n) as c on a.sid_md=c.sid_md and a.version_receta=c.version_receta
JOIN ${hiveconf:MY_SCHEMA}.iit_paciente d on a.sid_md=d.sid_md and a.version_receta=d.version_receta
JOIN (
select 
sid_md,
version_receta,
umf_desc,
delegacion_desc,
array_index( t.diagnostico, n ) as  diagnostico
from ( select sid_md, version_receta, umf_desc, delegacion_desc, diagnostico from ${hiveconf:MY_SCHEMA}.iit_satelite ) t
lateral view numeric_range( size( diagnostico )) n1 as n) as e on a.sid_md=e.sid_md and a.version_receta=e.version_receta
JOIN ${hiveconf:MY_SCHEMA}.iit_provenance as f on a.sid_md=f.sid_md and a.version_receta=f.version_receta) s 
where s.version_receta = 1;
