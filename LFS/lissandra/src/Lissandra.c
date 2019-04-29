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
#include "apiLFS.h"


int main(void) {

	logger = init_logger();	//HAY QUE ACORDARSE DE CERRAR EL LOGGER EN ALGUN MOMENTO
	config = read_config();
	memtable= list_create();

	char* ip=config_get_string_value(config, "IP");
	log_info(logger, ip);
	char* port=config_get_string_value(config, "PORT");
	log_info(logger, port);
	char* value=config_get_string_value(config, "TAMVALUE");
	log_info(logger, value);


	//PRUEBA SOCKETS CON LIBRERIA
/*
	u_int16_t  server;

	if(createServer(ip,atoi(port),&server)!=0){
		log_info(logger, "\nNo se pudo crear el server por el puerto o el bind");
	}else{
		log_info(logger, "\nSe pudo crear el server");
	}

	u_int16_t socket_client;

	listenForClients(server,100);

	char* serverName=config_get_string_value(config, "NAME");

	if(acceptConexion( server, &socket_client,serverName,1, atoi(value))!=0){
		log_info(logger, "\nError en el acept o al enviar handshake");
	}else{
		log_info(logger, "\nSe acepto la conexion");
	}

	char* buffer = malloc(sizeof(char)*11);

	if(recvData(socket_client, buffer,10)!=0){
		log_info(logger, "\nError al recibir la informacion");
	}else{
		log_info(logger, buffer);
	}
*/
	//PRUEBA DEL INSERT
	Registry *prueba;
	insert("tablita",2,"Mensaje",300);
	prueba= getList();
	log_info(logger,prueba->name);
	log_info(logger, prueba->value);
	//log_info(logger,itoa(prueba->key,,10));
	theEnd();
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
void theEnd(){
	list_destroy(memtable);
	log_destroy(logger);
	config_destroy(config);
}
