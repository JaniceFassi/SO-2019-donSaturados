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

	theStart();

	u_int16_t socket_client;

/*
    // ****************PARA USAR TIEMPO DEL DUMP*************

	alarm(timeDump);
    signal(SIGALRM, funcionSenial);

	// ***************PARA USAR LA FUNCION PURA****************/

	create("PELICULAS", "SC", 5, 10000);
	insert("PELICULAS", 163, "Nemo", 10);				// 3
	insert("PELICULAS", 10, "Toy Story",10);			// 0
	insert("PELICULAS", 10, "Harry Potter",16);			// 0
	insert("PELICULAS", 10, "La cenicienta",10);			// 0
	insert("PELICULAS", 10, "Monsters inc.",10);			// 0
	char *v=lSelect("PELICULAS",163);
	free(v);
	dump();
/*
	lSelect("PELICULAS", 163);					// Nemo
	insert("PELICULAS", 13535, "Titanic",20);			// 0
	lSelect("PELICULAS", 13535);					// Titanic
	insert("PELICULAS", 922, "Ratatouille",18);			// 2
	insert("PELICULAS", 4829,"Aladdin",10);				// 5
	insert("PELICULAS", 2516, "Godzilla",1300);			// 1
	insert("PELICULAS", 163, "Buscando a dory",1300);	// 1
	lSelect("PELICULAS", 4829);					// Aladdin
	insert("PELICULAS", 3671, "Avatar",1000);			// 1
	dump();
	lSelect("PELICULAS", 163);					// Buscando a dory
	lSelect("PELICULAS", 3671);					// Avatar
*/
	/*************************************************************/

	/****************PARA USAR LA CONSOLA******************/

	//console();
	//free(valor);

	/****************PARA USAR CONEXIONES******************/

	//connectMemory(&socket_client);

	int i=0;
	int recibidos=0;
	int header=0;

	/*while(i<2){
	char *buffer=malloc(2);
	recibidos=recvData(socket_client,buffer,1);
	header=atoi(buffer);
	exec_api(header,socket_client);
	free(buffer);
	i++;
	}*/

	theEnd();
	return EXIT_SUCCESS;
}

void theStart(){

	pathInicial=malloc(strlen("/home/utnso/tp-2019-1c-donSaturados/LFS/LFS.config")+1);		//Inicia variable global de path inicial
	strcpy(pathInicial,"/home/utnso/tp-2019-1c-donSaturados/LFS/LFS.config");
	raizDirectorio=malloc(strlen("/home/utnso")+1);
	strcpy(raizDirectorio,"/home/utnso");
	logger = init_logger();													//Inicia el logger global
	if(archivoValido(pathInicial)!=0){
		estructurarConfig();						//Si existe el config en el path inicial crea la estructura, si no crea el config
	}else{
		crearConfig();
	}
	levantarDirectorio();				//Crea el directorio ya teniendo el archivo config listo
	memtable= list_create();												//Inicia la memtable global
	directorio=list_create();
}

t_log* init_logger() {
	return log_create("lissandra.log", "Lissandra", 1, LOG_LEVEL_INFO);
}

t_config* init_config() {
	return config_create(pathInicial);
}

char* recibirDeMemoria(u_int16_t sock){
	char *tam=malloc(3);
	char * buffer;
	recvData(sock,tam,2);

	buffer=malloc(atoi(tam));
	recvData(sock,buffer,((atoi(tam))));

	free(tam);
	return buffer;
}

void exec_api(op_code mode,u_int16_t sock){

	char *buffer;
	char **subCadena;

	switch(mode){
	char *respuesta;
	case 0:								//orden: tabla, key

		log_info(logger,"\nSELECT");
		buffer=recibirDeMemoria(sock);
		subCadena=string_split(buffer, ";");

		int keyBuscada=atoi(subCadena[1]);
		char *valor=lSelect(subCadena[0],keyBuscada);

		if(valor==NULL){
			respuesta=string_from_format("01");
		}else
		{
			if(strlen(valor)<10){
				respuesta=string_from_format("0000%i%s",strlen(valor),valor);
			}else{
				if(configLissandra->tamValue<100){
					respuesta=string_from_format("000%i%s",strlen(valor),valor);
				}
			}
		}
		sendData(sock,respuesta,strlen(respuesta));
		free(respuesta);
		free(valor);
		break;

	case 1:
		log_info(logger,"\nINSERT");	//Este es el insert que viene con el timestamp
		buffer=recibirDeMemoria(sock);	//orden: tabla, key, value, timestamp
		subCadena=string_split(buffer, ";");
		int key=atoi(subCadena[1]);
		long time=atol(subCadena[3]);
		if(insert(subCadena[0], key,subCadena[2],time)==1){
			respuesta=string_from_format("11");
		}else
		{
			respuesta=string_from_format("10");
		}
		sendData(sock,respuesta,strlen(respuesta));
		free(respuesta);
		break;

	case 2:
		log_info(logger,"\nCREATE");	//orden: tabla, consistencia, particiones, tiempoCompactacion
		buffer=recibirDeMemoria(sock);
		subCadena=string_split(buffer, ";");
		int part=atoi(subCadena[2]);
		int timeCompact=atol(subCadena[3]);
		if(create(subCadena[0],subCadena[1],part,timeCompact)==1){
			respuesta=string_from_format("21");
		}else
		{
			respuesta=string_from_format("20");
		}
		sendData(sock,respuesta,strlen(respuesta));
		free(respuesta);
		break;

	case 3:
		log_info(logger,"\nDESCRIBE");	//orden: tabla
		buffer=recibirDeMemoria(sock);

		//completar


		break;

	case 4:
		log_info(logger,"\nDROP");		//orden: tabla
		buffer=recibirDeMemoria(sock);

		//drop(buffer);
		break;

	default:
		log_info(logger,"\nOTRO");
		break;


	}
	free(buffer);
	liberarSubstrings(subCadena);
}

void connectMemory(u_int16_t *socket_client){	//PRUEBA SOCKETS CON LIBRERIA
	u_int16_t  server;

	if(createServer(configLissandra->Ip,configLissandra->puerto,&server)!=0){
		log_info(logger, "\nNo se pudo crear el server por el puerto o el bind, %n", 1);
	}else{
		log_info(logger, "\nSe pudo crear el server.");
	}

	listenForClients(server,100);


	if(acceptConexion( server, socket_client,configLissandra->idEsperado)!=0){
		log_info(logger, "\nError en el acept.");
	}else
	{
		char *maxValue;
		log_info(logger, "\nSe acepto la conexion de %i con %i.",configLissandra->id,configLissandra->idEsperado);
		if(configLissandra->tamValue<10){
			maxValue=string_from_format("00%i",configLissandra->tamValue);
		}else{
			if(configLissandra->tamValue<100){
				maxValue=string_from_format("0%i",configLissandra->tamValue);
			}
		}
		sendData(*socket_client,maxValue,3);
		free(maxValue);
	}
}

void console(){
	char* linea;
	while(1){
		linea = readline(">");

		if(!strncmp(linea,"SELECT ",7))
		{
			char **subStrings= string_n_split(linea,3," ");
			char *valor=lSelect(subStrings[1],atoi(subStrings[2]));
			log_info(logger,valor);
			printf("%s\n",valor);
			liberarSubstrings(subStrings);
			free(valor);
		}

	 	if(!strncmp(linea,"INSERT ",7)){//INSERT "NOMBRE" 5/ "VALUE" 156876
	 		char **split= string_n_split(linea,4," ");
	 		int cantPalabras=0;
	 		int key= atoi(split[2]);
	 		char **cadena=string_split(split[3]," ");

	 		while(cadena[cantPalabras]!=NULL){			//Cuento la cantidad de palabras sin tener en cuenta la primera parte
	 			cantPalabras++;							// INSERT nombre key no se toma en cuenta
	 		}

	 		long timestamp=atol(cadena[cantPalabras-1]);

	 		if(timestamp==0){									//NO TIENE TIMESTAMP
	 			timestamp= time(NULL);
	 			printf("%s",split[3]);
	 			if(insert(split[1],key,split[3],timestamp)==0){
	 				printf("Se realizo el insert\n");			//Calculo el timestamp y el value es la cadena completa
	 			}else{
	 				printf("No se pudo realizar el insert\n");
	 			}
	 		}else{
	 			int base= string_length(cadena[0])+1;
	 			char *value=malloc(base);
	 			char *espacio=malloc(2);
	 			strcpy(espacio," ");
	 			strcpy(value,cadena[0]);

	 			for (int i=1;i<cantPalabras-1;i++){
	 				base +=strlen(espacio)+1;
	 				value=realloc(value,base);
	 				strcat(value,espacio);
	 				base += strlen(cadena[i])+1;
	 				value=realloc(value,base);
	 				strcat(value,cadena[i]);
	 			}

	 			if(insert(split[1],key,value,timestamp)==0){
	 				printf("Se realizo el insert\n");
	 			}else{
	 				printf("No se pudo realizar el insert\n");
	 			}

	 			free(espacio);
	 			free(value);
	 		}
	 		liberarSubstrings(cadena);
	 		liberarSubstrings(split);
	 	}

	 	if(!strncmp(linea,"CREATE ",7)){
			char **subStrings= string_n_split(linea,5," ");
			if(create(subStrings[1],subStrings[2],atoi(subStrings[3]),atol(subStrings[4]))==0){
				printf("se pudo crear la tabla\n");
			}else{
				printf("No se pudo crear la tablas\n");
			}
			liberarSubstrings(subStrings);
		}

		if(!strncmp(linea,"DESCRIBE ",9)){
			char **subStrings= string_n_split(linea,2," ");
			t_list *tablas;
			if(subStrings[1]==NULL){
				tablas=describe(subStrings[1],0);// 0 si no ponen nombre de una Tabla
			}else{
				tablas=describe(subStrings[1],1);//1 si ponen nombre de Tabla
			}
			liberarSubstrings(subStrings);
		}

		if(!strncmp(linea,"DROP ",5)){
			char **subStrings= string_n_split(linea,2," ");
			if(subStrings[1]==NULL){
				log_info(logger,"No se ingreso el nombre de la tabla.");
			}else{
				drop(subStrings[1]);
			}
			liberarSubstrings(subStrings);
		}

		if(!strncmp(linea,"exit",5)){
			free(linea);
			theEnd();
			break;
		}
		free(linea);
	}
}

void funcionSenial(int sig){
	log_info(logger,"Comienzo de dump");
	dump();
	log_info(logger,"Finalizacion de dump");
	alarm(configLissandra->tiempoDump);
}

void theEnd(){
	if(!list_is_empty(memtable)){
		list_destroy_and_destroy_elements(memtable,(void*)liberarTabla);
	}else{
		list_destroy(memtable);
	}
	borrarDatosConfig();
	borrarMetaLFS();
	free(pathInicial);
	free(raizDirectorio);
	bitarray_destroy(bitmap);
	close(archivoBitmap);
	log_destroy(logger);
}
