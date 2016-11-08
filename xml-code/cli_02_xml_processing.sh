#!/bin/bash
# cli_02_xml_processing.sh

# funcion para verificar el uso del script y sus opciones.
usage() {
	case $1 in
		"")
			echo ""
			echo "Usage: hive_scripts.sh command [options]"
			echo "      for info on each command: --> hive_scripts.sh command -h|--help"
			echo "Commands:"
			echo "      hive_scripts.sh push_with_hql_script [options]"
			echo "      hive_scripts.sh create_schema_hql_script [options]"
			echo ""
			echo ""
			;;
		push_with_hql_script)
			echo ""
			echo "Usage: cli_02_xml_processing.sh push_with_hql_script [-h|--help]"
			echo ""
			echo "  This is a quick example of using a hql script with a bash script to push ETL Process:"
			echo ""
			echo "Params:"
			echo "      -d|--database <database_name>"
			echo "      -t|--table: <table_name>"
			echo "      -h|--help: print this help info and exit"
			echo "Examples:"
			echo ""
			echo "		./cli_02_xml_processing push_with_hql_script -d a_schema_name -t table_name -l a_location_path"
			echo ""
			;;
		create_schema_hql_script)
			echo ""
			echo "Usage: cli_02_xml_processing.sh create_schema_hql_script [-h|--help]"
			echo ""
			echo "  This is a quick example of using a hql script with a bash script to push ETL Process:"
			echo ""
			echo "Params:"
			echo "      -d|--database <database_name>"
			echo "      -h|--help: print this help info and exit"
			echo "      -l|--location <path/to/location>"
			echo "Examples:"
			echo ""
			echo "		./cli_02_xml_processing create_schema_hql_script -d a_schema_name -l a_location_path"
			echo ""
			;;

	esac
	exit
}

# esta sección ejecuta el los commandos de HIVE con sus espesificación
args() {
	echo "processing command request"
    case $1 in
        push_with_hql_script)
            shift
            push_with_hql_script $@
            ;;
       create_schema_hql_script)
            shift
            create_schema_hql_script $@
            ;;

        *)
            echo >&2 "Invalid comand: $1"
            usage
        	;;
    esac
}

# función para crear las tablas Base e Incrementales 
create_schema_hql_script() {
	# init params
	database=""
	location=""
	begin_date=`date +%Y-%m-%d:%H:%M:%S`

	# process args for this block
	while test $# -gt 0
	do
    case $1 in
            -d|--database)
            	shift
            	database=$1
            	;;
            -h|--help)
            	usage create_schema_hql_script
            	;;
            -l|--location)
            	shift
            	location=$1
            	;;
        	*)
            	echo >&2 "Invalid argument: $1"
            	usage "create_schema_hql_script"
        	    ;;
    	esac
    	shift
	done
	
	# validamos si alguna opcion nos esta faltando	
	if [ x"$database" == "x" ]; then
		echo "missing database name: -d|--database <database_name>"
		usage "create_schema_hql_script"
	fi
	
	if [ x"$location" == "x" ]; then
		echo "missing location clause LOCATION : -l|--location <path/to/location>"
		usage "create_schema_hql_script"
	fi

	# imprimimos el comando que estamos ejecutando
	hive_script="SCH00-ALL_TABLES.hql"
	echo "hive -hiveconf MY_SCHEMA=$database -hiveconf MY_EBT_LOCATION=$location -f $hive_script"
	my_value=`hive -hiveconf MY_SCHEMA=$database -hiveconf MY_EBT_LOCATION=$location -f $hive_script`
	echo "returned value = $my_value"	
	exit
		
}

# función de para ejecutar el script que carga las tablas Incrementales 
push_with_hql_script() {
	# init params
	database=""
	table=""
	begin_date=`date +%Y-%m-%d:%H:%M:%S`

	# process args for this block
	while test $# -gt 0
	do
    case $1 in
            -d|--database)
            	shift
            	database=$1
            	;;
            -h|--help)
            	usage push_with_hql_script
            	;;
	    -t|--table)
            	shift
            	table=$1
            	;;
        	*)
            	echo >&2 "Invalid argument: $1"
            	usage "push_with_hql_script"
        	    ;;
    	esac
    	shift
	done
	
	# validamos si alguna opcion nos esta faltando	
	if [ x"$database" == "x" ]; then
		echo "missing database name: -d|--database <database_name>"
		usage "push_with_hql_script"
	fi
	
	if [ x"$table" == "x" ]; then
		echo "missing table name: -t|--table table_name"
		usage "pull_with_hql_script"
	fi

	# imprimimos el comando que estamos ejecutando
	hive_script="ETL02-LIT_$table.hql"
	echo "hive -hiveconf MY_SCHEMA=$database -f $hive_script"
	my_value=`hive -hiveconf MY_SCHEMA=$database -f $hive_script`
	echo "returned value = $my_value"	
	exit
		
}

# -------------------------------------------------------------------------------------
# Iniciamos ejecución de script
#

args $@
