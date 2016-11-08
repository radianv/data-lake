#!/bin/bash
# cli_01_xml_clean.sh

# funcion para verificar el uso del script y sus opciones.
usage() {
	case $1 in
		"")
			echo ""
			echo "Usage: cli_01_xml_clean.sh [-h|--help]"
			echo ""
			echo "  This is a quick example of using a hql script with a bash script to push ETL Process:"
			echo ""
			echo "Params:"
			echo "      -p|--path <path_to_xml>"
			echo "      -l|--location: <path_to_location>"
			echo "      -h|--help: print this help info and exit"
			echo "Examples:"
			echo ""
			echo "		./cli_01_xml_clean -p a_xml_path -l a_location_path"
			echo ""
			;;

	esac
	exit
}

# esta sección ejecuta el los commandos de HIVE con sus espesificación
args() {
	echo "processing command request"
        push_with_hdfs_script $@
}


# función de para ejecutar el script que carga las tablas Incrementales 
push_with_hdfs_script() {
	# init params
	path_x=""
	path_l=""
	begin_date=`date +%Y-%m-%d:%H:%M:%S`

	# process args for this block
	while test $# -gt 0
	do
    case $1 in
            -p|--path)
            	shift
            	path_x=$1
            	;;
            -h|--help)
            	usage
            	;;
	    -l|--location)
            	shift
            	path_l=$1
            	;;
        	*)
            	echo >&2 "Invalid argument: $1"
            	usage ""
        	    ;;
    	esac
    	shift
	done
	
	# validamos si alguna opcion nos esta faltando	
	if [ x"$path_x" == "x" ]; then
		echo "missing path name: -p|--path <path_to_xml>"
		usage ""
	fi
	
	if [ x"$path_l" == "x" ]; then
		echo "missing location name: -l|--location: <path_to_location>"
		usage ""
	fi

        # imprimimos el comando que estamos ejecutando
	cd $path_x
        for i in $(ls *.tar.gz);
        do
          echo $i
          zcat $i sed 's/xmlns="[^>]*"//g' | tar xvf - --strip=4
        done

	#imprimimos el comando que estamos ejecutando
        for e in $(ls *.xml);
        do
          echo $e
          hdfs dfs -put $e $path_l
        done    
        rm *.xml
        exit
		
}

# -------------------------------------------------------------------------------------
# Iniciamos ejecución de script
#

args $@
