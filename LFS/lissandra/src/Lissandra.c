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


/*	u_int16_t socket_client;

	//CREACION DE LA CARPETA PRINCIPAL DE TABLAS



	// ****************PARA USAR TIEMPO DEL DUMP*************

	//
	//alarm(timeDump);
    //signal(SIGALRM, funcionSenial);

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

	// ***************PARA USAR LA FUNCION PURA****************/

	create("PELICULAS", "SC", 5, 10000);
	insert("PELICULAS", 163, "Nemo", 100);				// 3
	insert("PELICULAS", 10, "Toy Story",10);			// 0
	insert("PELICULAS", 10, "Harry Potter",16);			// 0
	insert("PELICULAS", 10, "La cenicienta",10);			// 0
	insert("PELICULAS", 10, "Monsters inc.",10);			// 0
	create("COMIDAS", "SH", 2, 10000);
	//create("TERMINA", "SH", 4, 10000);
	//create("VIVE", "SH", 2, 10000);
	insert("COMIDAS", 10, "Toy Story",10);			// 0
	insert("COMIDAS", 10, "Harry Potter",10);			// 0
//	selectS("PELICULAS",10);
	//dump();
	newSelect("PELICULAS", 10);				// Harry Potter
	//selectS("PELICULAS", 163);					// Nemo
	insert("PELICULAS", 13535, "Titanic",20);			// 0
	//selectS("PELICULAS", 13535);					// Titanic
	insert("PELICULAS", 922, "Ratatouille",18);			// 2
	insert("PELICULAS", 4829,"Aladdin",10);				// 5
	insert("PELICULAS", 2516, "Godzilla",1300);			// 1
	insert("PELICULAS", 163, "Buscando a dory",1300);	// 1
	//selectS("PELICULAS", 4829);					// Aladdin
	insert("PELICULAS", 3671, "Avatar",1000);			// 1
	dump();
	newSelect("PELICULAS", 163);					// Buscando a dory
	//selectS("PELICULAS", 3671);					// Avatar

	/*************************************************************/

	/****************PARA USAR LA CONSOLA******************/

	//console();
	//free(valor);

	/****************PARA USAR CONEXIONES******************/

	//connectMemory(&socket_client);

	//int i=0;
	//while(i<5){
	/*char *buffer=malloc(2);
	int recibidos=recvData(socket_client,buffer,1);

	exec_api(atoi(buffer),socket_client);
	free(buffer);*/
	//i++;
	//}

	theEnd();
	return EXIT_SUCCESS;
}

void theStart(){
	pathInicial="/home/utnso/tp-2019-1c-donSaturados/LFS/LFS.config";		//Inicia variable global de path inicial
	raizDirectorio="/home/utnso";
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
	}else{
		log_info(logger, "\nSe acepto la conexion de %i con %i.",configLissandra->id,configLissandra->idEsperado);
		//aca deberia mandar el tamaño maximo del valor
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
	bitarray_destroy(bitmap);
	close(archivoBitmap);
	log_destroy(logger);
}
