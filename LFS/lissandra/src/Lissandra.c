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

	theStart();

	u_int16_t socket_client;

	//CREACION DE LA CARPETA PRINCIPAL DE TABLAS

	puntoMontaje= config_get_string_value(config,"PUNTO_MONTAJE");

	char *path=pathFinal("Tablas",0);	//DEVUELVE EL PATH HASTA LA CARPETA TABLAS

	if(folderExist(path)!=0){		//SI NO EXISTE LA CARPETA TABLAS DEL FILESYSTEM LA CREA
		if(crearCarpeta(path)!=0){
			free(path);
			theEnd();
			return 1;
		}
		log_info(logger,"\nSe ha creado la carpeta principal.");
	}

	free(path);

	//connectMemory(&socket_client);

	//int i=0;
	//while(i<5){
	/*char *buffer=malloc(2);
	int recibidos=recvData(socket_client,buffer,1);

	exec_api(atoi(buffer),socket_client);
	free(buffer);*/
	//i++;
	//}

	console();

	theEnd();
	return EXIT_SUCCESS;
}

t_log* init_logger() {
	return log_create("lissandra.log", "Lissandra", 1, LOG_LEVEL_INFO);
}

t_config* read_config() {
	return config_create("/home/utnso/tp-2019-1c-donSaturados/LFS/LFS.config");
}

char* recibirDeMemoria(u_int16_t sock){
	char *tam=malloc(3);
	char * buffer;
	recvData(sock,tam,2);

	buffer=malloc(atoi(tam));
	recvData(sock,buffer,((atoi(tam))));

	free(tam);
	log_info(logger,buffer);
	return buffer;
}

void exec_api(op_code mode,u_int16_t sock){

	char *buffer;
	char **subCadena;

	switch(mode){
	case 0:								//orden: tabla, key

		log_info(logger,"\nSELECT");
		buffer=recibirDeMemoria(sock);
		log_info(logger,buffer);
		subCadena=string_split(buffer, ";");
		log_info(logger,subCadena[0]);
		log_info(logger,subCadena[1]);
		/*char *valor=selectS(subCadena[0],atoi(subCadena[1]));
		printf("\n%s",valor);
		log_info(logger,valor);*/
		break;

	case 1:
		log_info(logger,"\nINSERT");	//Este es el insert que viene con el timestamp
		buffer=recibirDeMemoria(sock);	//orden: tabla, key, value, timestamp
		log_info(logger,buffer);
		subCadena=string_split(buffer, ";");
		log_info(logger,subCadena[0]);
		log_info(logger,subCadena[1]);
		log_info(logger,subCadena[2]);
		log_info(logger,subCadena[3]);
		insert(subCadena[0], atoi(subCadena[1]),subCadena[2],atol(subCadena[3]));

		break;

	case 2:
		log_info(logger,"\nCREATE");	//orden: tabla, consistencia, particiones, tiempoCompactacion
		buffer=recibirDeMemoria(sock);
		log_info(logger,buffer);
		subCadena=string_split(buffer, ";");
		log_info(logger,subCadena[0]);
		log_info(logger,subCadena[1]);
		log_info(logger,subCadena[2]);
		log_info(logger,subCadena[3]);
		create(subCadena[0],subCadena[1],atoi(subCadena[2]),atol(subCadena[3]));
		break;

	case 3:
		log_info(logger,"\nDESCRIBE");	//orden: tabla
		buffer=recibirDeMemoria(sock);
		log_info(logger,buffer);
		//completar
		break;

	case 4:
		log_info(logger,"\nDROP");		//orden: tabla
		buffer=recibirDeMemoria(sock);
		log_info(logger,buffer);

		//drop(buffer);
		break;

	default:
		log_info(logger,"\nOTRO");
		break;
	}
	free(buffer);
	//free(subCadena);
}
void theStart(){
	logger = init_logger();
	config = read_config();
	memtable= list_create();
}
void connectMemory(u_int16_t *socket_client){	//PRUEBA SOCKETS CON LIBRERIA
	u_int16_t  server;
	char* ip=config_get_string_value(config, "IP");
	//log_info(logger, ip);
	u_int16_t port= config_get_int_value(config, "PORT");
	//log_info(logger, "%i",port);
	u_int16_t maxValue= config_get_int_value(config, "TAMVALUE");
	//log_info(logger, "%i",maxValue);
	u_int16_t id= config_get_int_value(config, "ID");
	//log_info(logger, "%i",id);
	u_int16_t idEsperado= config_get_int_value(config, "IDESPERADO");
	//log_info(logger, "%i",idEsperado);

	if(createServer(ip,port,&server)!=0){
		log_info(logger, "\nNo se pudo crear el server por el puerto o el bind, %n", 1);
	}else{
		log_info(logger, "\nSe pudo crear el server");
	}

	listenForClients(server,100);

	//char* serverName=config_get_string_value(config, "NAME");

	if(acceptConexion( server, socket_client,idEsperado)!=0){
		log_info(logger, "\nError en el acept");
	}else{
		log_info(logger, "\nSe acepto la conexion de %i con %i",id,idEsperado);
	}

}

void console(){
	char* linea;
	while(1){
		linea = readline(">");

		if(!strncmp(linea,"SELECT ",7))
		{
			char **subStrings= string_n_split(linea,3," ");
			char *valor=selectS(subStrings[1],atoi(subStrings[2]));
			printf("%s",valor);
		}
	 	if(!strncmp(linea,"INSERT ",7)){
	 		char **subStrings= string_n_split(linea,5," ");
	 		if(subStrings[4]==NULL){
	 			long timestamp= time(NULL);
	 			int key=atoi(subStrings[2]);
	 			insert(subStrings[1],key,subStrings[3],timestamp);
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
void dump(){
	t_list *dump=list_duplicate(memtable);
	list_clean(memtable);
	int cant=list_size(dump);
	while(dump>=0){
		Tabla *dumpT=list_get(dump,cant-1);
		char *path=pathFinal(dumpT->nombre,1);
		if(folderExist(path)==0){
			free(path);
			path=pathFinal(dumpT->nombre,3);
			metaTabla *metadata= leerArchMetadata(path);
			free(path);
			t_list *regDepurados=regDep(dumpT->registros);
			escribirReg(dumpT->nombre,regDepurados,metadata->partitions);
			free(metadata);
			list_destroy_and_destroy_elements(regDepurados,(void *)destroyRegistry);//no se si tengo q liberar los registros tambn
			liberarTabla(dumpT);
		}else{
			liberarTabla(dumpT);
			free(path);

		}

	}
}

void theEnd(){
	list_destroy_and_destroy_elements(memtable,(void *)liberarTabla);
	log_destroy(logger);
	config_destroy(config);
}
