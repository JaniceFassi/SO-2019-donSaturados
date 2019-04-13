/*
 ============================================================================
 Name        : Lissandra.c
 Author      : jani_sol
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */
#include "Lissandra.h"

int main(void) {

	struct sockaddr_in direccionServidor;
	t_log* logger = init_logger();	//HAY QUE ACORDARSE DE CERRAR EL LOGGER EN ALGUN MOMENTO
	t_config* config = read_config();

	//memtable = list_create();	//NO SE SI ESTO DEBERIA ESTAR ACA

	direccionServidor.sin_family=AF_INET;
	direccionServidor.sin_addr.s_addr=INADDR_ANY;
	direccionServidor.sin_port=htons(8000);

	int servidor= socket(AF_INET,SOCK_STREAM,0);

	//INICIO DEL SERVIDOR
	//VER SI SE PUEDE HACER EN UNA FUNCION APARTE, PARA NO TENERLO EN EL MAIN
	if(bind(servidor, (void*) &direccionServidor, sizeof(direccionServidor))!=0){
		perror("Fallo el bind");
		log_info(logger, "Error al iniciar el servidor");
		return 1;
	}

	listen(servidor,100);
	printf("Estoy escuchando");
	log_info(logger, "Servidor escuchando");

	char* value = config_get_string_value(config, "PUNTO_MONTAJE");
	log_info(logger, value);

	return EXIT_SUCCESS;
}

t_log* init_logger() {
	return log_create("lissandra.log", "Lissandra", 1, LOG_LEVEL_INFO);
}

t_config* read_config() {
	return config_create("/home/utnso/tp-2019-1c-donSaturados/LFS/LFS.config");
}

void exec_api(op_code mode){	//Averiguar si se puede hacer asi, segun el header que le llegue llama a la funcion correspondiente (en api o fs??)

	switch(mode){				//Tendria que haber una funcion comun. Ver ejemplo en las commons log que hay funciones template

	case 0:
		break;
	case 1:
		break;
	case 2:
		break;
	case 3:
		break;
	case 4:
		break;
	default:
		break;
	}
}

