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

	char *path=pathFinal("TABLAS",0);	//DEVUELVE EL PATH HASTA LA CARPETA TABLAS

	if(folderExist(path)!=0){		//SI NO EXISTE LA CARPETA TABLAS DEL FILESYSTEM LA CREA
		if(crearCarpeta(path)!=0){
			free(path);
			theEnd();
			return 1;
		}
		log_info(logger,"\nSe ha creado la carpeta principal.");
	}

	free(path);

	create("tab","SC",3,300);

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
		log_info(logger, "\nSe pudo crear el server.");
	}

	listenForClients(server,100);

	//char* serverName=config_get_string_value(config, "NAME");

	if(acceptConexion( server, socket_client,idEsperado)!=0){
		log_info(logger, "\nError en el acept.");
	}else{
		log_info(logger, "\nSe acepto la conexion de %i con %i.",id,idEsperado);
	}

}

int esDigito(int ascii){		//Si es numero retorna 0, si no 1
	if(ascii >47 && ascii <58){
		return 0;
	}else return 1;
}

void console(){
	char* linea;
	while(1){
		linea = readline(">");

		if(!strncmp(linea,"SELECT ",7))
		{
			char **subStrings= string_n_split(linea,3," ");
			char *valor=selectS(subStrings[1],atoi(subStrings[2]));
			log_info(logger,valor);
		}

	 	if(!strncmp(linea,"INSERT ",7)){
	 		char **split= string_n_split(linea,4," ");
	 		int cantPalabras=0;
	 		char **cadena=string_split(split[3]," ");
	 		while(cadena[cantPalabras]!=NULL){			//Cuento la cantidad de palabras sin tener en cuenta la primera parte
	 			cantPalabras++;							// INSERT nombre key no se toma en cuenta
	 		}
	 		/*char *letra=string_substring(cadena[cantPalabras], 0, 1);
	 		if(esDigito(toascii(*letra))==0){		//Si la ultima letra de la ultima palabra es numero tiene timestamp
	 			char *value=malloc(string_length(cadena[0]));
	 			strcpy(value,cadena[0]);
	 			for (int i=1;i<cantPalabras-1;i++){
	 				strcat(value,cadena[i]);
	 			}
	 			insert(split[1],atoi(split[2]),value,cadena[cantPalabras]);
	 		}
	 		else{										//Si no, no tiene timestamp
	 			long timestamp= time(NULL);
	 			log_info(logger,split[3]);
	 			insert(split[1],atoi(split[2]),split[3],timestamp);	//Calculo el timestamp y el value es la cadena completa
	 		}			YA CASI		*/

	 		/*	//si es mayor a 4 el split hay que concatenar...
	 		if(cantPalabras>4){
	 			printf("%i\n",cantPalabras);
	 			printf("%s\n",split[cantPalabras-1]);
	 			int cantPalabrasMenosUno=atoi(split[cantPalabras-1]);
	 			printf("%i\n",cantPalabrasMenosUno);
	 			char*conc=malloc(19);
	 			strcpy(conc,"");
	 			if(cantPalabrasMenosUno==0){
	 				int i=3;
	 				while(i<cantPalabras){
	 					string_append(&conc,split[i]);
	 					string_append(&conc," ");
	 					i++;
	 				}
	 			}
	 			else{
	 				int i=3;
	 			while(i<cantPalabras-1){
	 					string_append(&conc,split[i]);
	 					string_append(&conc," ");
	 					i++;
	 				}
	 			}
	 				insert(split[1],split[2],&conc,split[4]);
	 			}*/
// en caso de no serlo el ultimo elemento del split es el value
// si es menor a 4 deberia romper, manejense con eso

	 		/*char **subStrings= string_n_split(linea,5," ");
	 		if(subStrings[4]==NULL){
	 			long timestamp= time(NULL);
	 			int key=atoi(subStrings[2]);
	 			insert(subStrings[1],key,subStrings[3],timestamp);
	 		}else{
	 			insert(subStrings[1], atoi(subStrings[2]),subStrings[3],atol(subStrings[4]));
*/
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
				log_info(logger,"No se ingreso el nombre de la tabla.");
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
	while(cant>0){
		Tabla *dumpT=list_get(dump,cant-1);
		char *path=pathFinal(dumpT->nombre,1);
		if(folderExist(path)==0){
			free(path);
			path=pathFinal(dumpT->nombre,3);
			metaTabla *metadata= leerArchMetadata(path);
			t_list *regDepurados=regDep(dumpT->registros);
			list_destroy(dumpT->registros);
			escribirReg(dumpT->nombre,regDepurados,metadata->partitions);
			free(metadata->consistency);
			free(metadata);
			list_destroy_and_destroy_elements(regDepurados,(void *)destroyRegistry);
		}
		free(path);
		free(dumpT->nombre);
		free(dumpT);
		dumpT=NULL;
		cant--;
	}
	list_destroy(dump);
}

void theEnd(){
	list_destroy_and_destroy_elements(memtable,(void *)liberarTabla);
	log_destroy(logger);
	config_destroy(config);
}
