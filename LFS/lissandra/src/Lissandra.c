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

char *valgrind;

int main(void) {

	//PRUEBA VALGRIND
	//valgrind = malloc(8);

	theStart();
	//CREACION DE LA CARPETA PRINCIPAL DE TABLAS
	//puntoMontaje= config_get_string_value(config,"PUNTO_MONTAJE");
	//char *path=pathFinal("Tablas",0,puntoMontaje);
	/*if(folderExist(path)==0){
		log_info(logger,"LA CARPETA PRINCIPAL YA EXISTE");
		free(path);
		theEnd();
		return 1;
	}*/

	/*if(crearCarpeta(path)!=0){

		free(path);
		theEnd();
		return 1;
	}
	free(path);*/
	connectMemory();

	//PRUEBA DE INSERT Y SELECT
	//char* valor=malloc(255);

	//insert("tablita1",0,"Mensaje1",300);  //MIENTRAS QUE NO HAYA FILE SYSTEM, LA KEY ES EL INDEX DE MEMTABLE
	//insert("tablita2",1,"Mensaje2",300); //Y EL NOMBRE DE LA TABLA ES EL NOMBRE DEL ARCHIVO

	//selectS("tablita1",0,valor);

	//console();
	//free(valor);
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
void theStart(){
	logger = init_logger();
	config = read_config();
	memtable= list_create();
}
void connectMemory(){	//PRUEBA SOCKETS CON LIBRERIA
	u_int16_t  server;
	u_int16_t socket_client;
	char* ip=config_get_string_value(config, "IP");
	log_info(logger, ip);
	u_int16_t port= config_get_int_value(config, "PORT");
	log_info(logger, "%i",port);
	u_int16_t value= config_get_int_value(config, "TAMVALUE");
	log_info(logger, "%i",value);

	if(createServer(ip,port,&server)!=0){
		log_info(logger, "\nNo se pudo crear el server por el puerto o el bind, %n", 1);
	}else{
		log_info(logger, "\nSe pudo crear el server");
	}

	listenForClients(server,100);

	char* serverName=config_get_string_value(config, "NAME");

	if(acceptConexion( server, &socket_client,serverName,1, value)!=0){
		log_info(logger, "\nError en el acept o al enviar handshake");
	}else{
		log_info(logger, "\nSe acepto la conexion");
	}

	char* buffer = malloc(16);
	int tamanio= 15;
	memset(buffer,'\0',tamanio+1);

	if(recvData(socket_client, buffer,tamanio)!=0){
		log_info(logger, "\nError al recibir la informacion");
	}else{

		printf("%s",buffer);
		log_info(logger, buffer);

	}
}
void console(){
	char* linea;
	while(1){
		linea = readline(">");

		if(!strncmp(linea,"SELECT ",7))
		{
			char *valor;
			char **subStrings= string_n_split(linea,3," ");
			selectS(subStrings[1],atoi(subStrings[2]),valor);
			printf("%s",valor);
		}
	 	if(!strncmp(linea,"INSERT ",7)){
	 		char **subStrings= string_n_split(linea,5," ");
	 		if(subStrings[3]==NULL){
	 			long timestamp= time(NULL);
	 			insert(subStrings[1], atoi(subStrings[2]),subStrings[3],timestamp);
	 		}else{
	 			insert(subStrings[1], atoi(subStrings[2]),subStrings[3],atol(subStrings[4]));

	 		}
	 	}
		if(!strncmp(linea,"CREATE ",7)){
			char **subStrings= string_n_split(linea,5," ");
			create(subStrings[1],subStrings[2],atoi(subStrings[3]),atol(subStrings[4]));

		}
		if(!strncmp(linea,"DESCRIBE ",9)){
			char **subStrings= string_n_split(linea,2," ");
			t_list *tablas;
			if(subStrings[1]==NULL){
				describe(subStrings[1],tablas,0);// 0 si no ponen nombre de una Tabla
			}else{
				describe(subStrings[1],tablas,1);//1 si ponen nombre de Tabla
			}

		}
		if(!strncmp(linea,"DROP ",5)){
			char **subStrings= string_n_split(linea,2," ");
			if(subStrings[1]==NULL){
				log_info(logger,"No se ingreso el nombre de la tabla");
			}else{
				drop(subStrings[1]);
			}
		}

		if(!strncmp(linea,"exit",5)){
			free(linea);
			theEnd();
			break;
		}
		free(linea);
	}
}
void theEnd(){
	list_destroy(memtable);
	char *path=pathFinal("Tablas",0,puntoMontaje);
	if(folderExist(path)==0){
		borrarCarpeta(path);
		free(path);
	}
	free(path);
	log_destroy(logger);
	config_destroy(config);

}
